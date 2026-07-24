# KubeBrain DBaaS 兼容性计划

本文定义 `dbaas` 分支的产品边界和验收标准。目标不是复刻 etcd 的
Raft、bbolt 和成员管理内部实现，而是对通用 etcd v3 客户端提供可依赖的
数据语义，并用 TiKV/PD 的原生能力替代不适用的维护操作。

对标源码固定为：

- KubeBrain：本仓库 `dbaas` 分支；
- etcd：`/root/etcd`，首次基线提交 `d947b2086`；
- API：`go.etcd.io/etcd/api/v3 v3.7.0`；
- 消费端验证：官方 `client/v3`、`etcdctl`，以及 `/root/etcd` 中可复用的
  集成测试。Kubernetes 和 Cilium 套件继续作为回归集，而不是完整兼容性的
  唯一依据。

## DBaaS 产品边界

一个售卖实例由独立的 KubeBrain、PD 和 TiKV 集群组成，拥有独立 endpoint、
证书、资源配额、持久卷、备份策略和计量数据。共享 TiKV 只适用于开发或低
保障套餐，不作为生产默认方案。已有 tenant keyspace 保留为内部能力，但不
作为生产 DBaaS 的计费或故障隔离边界。

兼容性分为四类：

- **兼容**：客户端可依赖 etcd 的响应、错误、revision 和原子性语义；
- **部分兼容**：已覆盖 Kubernetes/Cilium 或常见请求形状，但通用形状仍有差距；
- **平台替代**：etcd RPC 对 TiKV/PD 架构不适用，由 DBaaS 控制面提供等价能力；
- **缺失**：通用 DBaaS 需要，但当前尚未实现。

## 当前矩阵

| 服务 | 能力 | 当前状态 | DBaaS 处理 |
| --- | --- | --- | --- |
| KV | Range/Put/DeleteRange | 兼容核心语义 | 排序、过滤、历史读、大范围删除原子性、生产范围上限、非 KEY/NONE/Limit 候选窗口及 KeysOnly+CountOnly 优先级差分已补齐；当前无已知语义差异，继续扩大生成式输入与长时故障 soak |
| KV | Txn | 兼容核心语义 | 缺失键 guard、范围 phantom guard、嵌套分支、staged 单 revision、写前错误验证及 caller deadline 贯穿后端冲突重试已完成；当前无已知语义差异，继续扩大生成式嵌套矩阵与多点故障 soak |
| KV | Compact | 兼容核心语义 | logical/physical、错误、异步 GC 与请求取消后的后台续扫已对齐；继续长时间故障 soak |
| KV | RangeStream | 兼容核心语义 | etcd 3.7 支持的 CountOnly/Limit/KeysOnly/默认排序已对齐；自定义排序与 revision filter 同 etcd 明确 Unimplemented |
| Watch | create/cancel/progress/history/prevKV/slow-consumer catch-up | 兼容核心语义；后端溢出无缝追赶，控制响应不阻塞接收循环 | P1：继续数天级断线/慢消费者 soak |
| Lease | grant/revoke/keepalive/ttl/list | 兼容核心语义 | meta/attachment 已与用户 revision 隔离并原子提交，Grant durable 后才发布，List 按到期时间稳定排序；异步 revoke 被阻断时 TTL 与 etcd 一样持续为负；继续扩大故障、并发和错误差分矩阵 |
| Auth | 用户、角色、权限、token | 兼容核心语义 | 管理 API、key-range RBAC、token 生命周期、Watch/Lease 持续鉴权及多副本故障转移已验证 |
| Cluster | MemberList | 兼容（需配置） | DBaaS 通过 `--initial-cluster` 注入完整 KubeBrain peer 身份，并用 `--advertise-client-urls` 独立发布所有 clientv3 Sync/AutoSync 调用方可达且匹配 TLS SAN 的 client endpoint；未配置静态成员时仅返回本机与 leader 的降级视图 |
| Cluster | add/remove/update/promote | 平台替代 | 由 DBaaS 控制面扩缩 KubeBrain、PD、TiKV；RPC 保持明确 Unimplemented |
| Maintenance | Status | 兼容核心语义 | 返回真实服务身份、版本、leader/revision/共享选主 term；配置 quota 时报告租户最新逻辑 key+value 字节，未配置时使用兼容 sentinel；TiKV 物理容量转到实例指标 |
| Maintenance | Snapshot | 平台替代 | 使用 TiKV BR/PITR；控制面提供备份、恢复和导出任务，不伪造 etcd snapshot |
| Maintenance | Defragment | 平台替代 | TiKV GC/compaction 管理，不执行 bbolt 碎片整理 |
| Maintenance | Alarm/DbSize | 兼容 NOSPACE/CORRUPT 核心语义 | keyspace 级逻辑容量原子计量、sticky NOSPACE、持久 member 集合及跨 endpoint mutation 已支持；NOSPACE 阻断增长写与 LeaseGrant，但允许 LeaseRevoke、自然过期和既有 lease KeepAlive，以保留空间自愈路径；CORRUPT 使用 TiKV 内部元数据持久 member 集合，允许 Range 及空/线性/串行只读 Txn 诊断，阻断 Put/Delete/任一分支含写的 Txn/Compact/LeaseGrant/Revoke，并推迟自然租约过期直至 disarm；未过期 lease 仍可 KeepAlive，已过期 KeepAlive 等待 revoke 并在 disarm 后返回 TTL=0；双告警并存时 CORRUPT 优先，逐项解除后 NOSPACE 独立延续；3 KubeBrain、3 PD、3 TiKV 全成员顺序重启后告警、写门禁和 health/readyz 状态仍持续；bbolt fragmentation 仍为平台边界 |
| Maintenance | Hash/HashKV | 兼容核心语义 | 对指定 revision 的租户 MVCC 实际内容做稳定摘要；成员本地诊断不依赖 KubeBrain leader，数值不与 bbolt 内部编码比较 |
| Maintenance | MoveLeader/Downgrade | 平台替代 | 分别由服务选主和 DBaaS 升级编排处理 |
| Endpoint | health/livez/readyz | 兼容核心语义 | `/health`、`/livez`、`/readyz` 及分项检查已对齐；`data_corruption`/`non_learner` 使用 TiKV 架构等价语义，`/ready` 与 `/ping` 为平台探针 |
| Endpoint | v3 JSON/HTTP gateway | 兼容核心服务 | 默认启用 KV、Watch、Lease、Cluster、Maintenance、Auth、Lock、Election generated gateway；经本机 gRPC 回环保留 admission、auth、metrics、限流和 TLS 语义，可用 `--enable-grpc-gateway=false` 关闭 |
| Concurrency | Lock/Election/STM recipes | 兼容核心语义 | 官方 `client/v3/concurrency` Mutex/Election/session/STM、orphan session lease 自然过期接棒、STM 冲突重试/守恒争用及真实 Leader 故障转移已通过；继续长时间 soak |

`Status.Version = 3.7.0` 只表示协议能力门槛，不能作为完整兼容声明。发布说明必须
引用本矩阵和自动化兼容测试结果。

## 实施顺序

### P0：数据正确性

1. 从 `/root/etcd/server/etcdserver/txn` 和 `api/v3rpc/key.go` 提取 KV/Txn
   行为表，建立同一组请求分别访问 etcd 与 KubeBrain 的差分测试。
2. 补齐通用 Txn：compare 读取与写提交必须处于同一可串行化判定中；成功
   分支全部写操作共享一个 revision；失败不得部分提交。
3. 覆盖 Range、DeleteRange、Watch、Lease、Compact 的错误码、header、历史
   revision 和边界输入。
4. 在真实 TiKV/PD 三节点后端运行故障注入和线性一致性检查。

P0 完成标准：官方 client/v3 的核心 KV/Watch/Lease/Txn 行为矩阵无已知语义
差异；差分、race、故障转移和持久化复读均通过。

### P0 实施记录

- **point compare 的不存在 guard**：对照
  `/root/etcd/server/etcdserver/txn/txn.go` 的 `applyCompare` 和同一 MVCC
  写事务执行方式，KubeBrain 现把“键不存在”也作为 `TxnGuard` 状态提交。
  `Version/CreateRevision(key)==0` 后创建同键，以及 compare 键与写键不同的
  事务，都会在同一个存储 batch 中断言缺失或精确 tombstone；并发创建会使
  整个事务重试并重新选择分支。覆盖测试：
  `TestTxnApplyAbsentGuardOverlappingPut`、
  `TestTxnApplyAbsentGuardDisjointFromWrite`、
  `TestTxnApplyAbsentGuardAcceptsTombstone`。
- **范围 compare phantom guard**：TiKV 当前采用 Snapshot Isolation，范围
  `Iter` 不提供 predicate lock；给每次写维护一个持久化全局 epoch 又会制造
  单 key 热点并串行化所有 TiKV 写。KubeBrain 因此使用 leader-local 逻辑写
  屏障：普通 Create/Update/Delete/DeleteRange/TxnApply 和 uncertain retry 持
  共享锁，仍可完全并发；仅含范围 compare（包括嵌套分支）的 generic txn 从
  compare 到 commit 持独占锁。领导权 epoch fence 继续阻止旧 leader 跨任期
  提交。`TestTxnRangeCompareExcludesPhantomInsert` 在 compare 扫描后并发插入
  区间键，证明插入必须等事务提交后才能完成；backend 层
  `TestRangeTxnBarrierBlocksExternalWrites` 和两者的 race 测试覆盖锁边界。代价
  是范围 compare 执行期间同一实例的逻辑写会短暂排队，因此 DBaaS 必须保留
  txn op/range 和 RPC deadline 限制。
- **nested point-write 原子路径**：选中分支可递归扁平化任意层 nested txn；
  当路径只含 distinct-key Put 和单键 DeleteRange 时，所有层级 compare guard
  与写操作一次提交、共享一个 revision，再按原树形重建 TxnResponse。事务内
  Put 的 `PrevKv` 同时补齐。`TestTxnNestedWriteOnlyUsesSingleRevision` 和
  `TestTxnAtomicPutReturnsPrevKV` 覆盖。
- **generic txn staged view**：当选中路径包含 Range、multi-key 或重叠
  DeleteRange、多个 DeleteRange、IgnoreValue/IgnoreLease 时，先在固定
  `baseRev` 上构建事务内视图，按请求顺序让后续读看到先前写，再把最终每键
  状态去重为一个 `TxnApply` batch。有效写共享一个 revision，任一校验失败不
  提交；空 DeleteRange 不消耗 revision。显式历史 Range 读取请求 revision 的
  数据，但响应 header 使用事务当前可见 revision，与
  `/root/etcd/server/storage/mvcc/kvstore_txn.go` 一致。排序、revision 过滤、
  limit/More、CountOnly、KeysOnly 均在 staged view 上执行。覆盖测试：
  `TestTxnRangeSeesPriorWritesButNotLaterWrites`、
  `TestTxnOverlappingDeleteRangesUseStagedViewAndOneRevision`、
  `TestTxnIgnoreOptionsUseStagedAtomicValidation`、
  `TestTxnHistoricalRangeAfterPutReadsOldValueWithTxnHeader`、
  `TestTxnNoOpDeleteRangesDoNotConsumeRevision`、
  `TestTxnRangeOptionsApplyToStagedView`。官方 client/v3 黑盒新增
  `TestTxnRangeUsesOrderedStagedView` 和
  `TestTxnOverlappingDeleteRangesShareOneRevision`，在 kind Kubernetes
  v1.36.1 + PD/TiKV v8.5.3 上连续 10 轮通过；完整 client 兼容套件和基础
  smoke 通过。TiKV persistence smoke 已直接读取 revision index 和 object
  value，并在 KubeBrain 重启后复读成功。commit-undetermined 的事务级判定、
  compaction pin 及 PD/TiKV 故障注入已由 A74、A99 和多键 Porcupine 历史补齐；
  继续扩大多 store/多 PD 拓扑的网络分区覆盖。
- **官方 etcd 双端差分**：新增
  `TestTxnDifferentialAgainstReferenceEtcd`，同一场景分别访问 TiKV-backed
  KubeBrain 和从 `/root/etcd` 提交 `d947b2086` 构建的参考 server。测试不比较
  cluster/member ID 或绝对 revision，而是结构化比较相对 revision、响应树、
  Range header、KV create/mod/version、PrevKV、CountOnly 以及 gRPC
  code/message，避免把两个独立数据库的预期差异误报为兼容问题。首轮差分发现
  IgnoreValue/IgnoreLease 目标不存在时 KubeBrain 错误返回 `NotFound` 和自定义
  文本；已对齐官方
  `/root/etcd/api/v3rpc/rpctypes/error.go` 的
  `InvalidArgument: etcdserver: key not found`。修复后的双端差分连续 10 轮通过。
  第二组 `TestRangeDifferentialAgainstReferenceEtcd` 覆盖顺序写 revision、
  历史快照、排序、revision 过滤、limit/More、KeysOnly、未来 revision 错误和
  空 DeleteRange。它发现带 revision filter 时 etcd 的 `Count` 是过滤前范围
  总键数，而 KubeBrain 曾错误返回过滤后 KV 数；已对齐
  `/root/etcd/server/etcdserver/txn/range.go`：过滤只裁剪 `KVs`，保留 MVCC
  range 预先计算的 Count，且没有新增扫描。修复后 Range 与 Txn 双端差分共同
  连续 10 轮通过。该段保留首轮差分背景；DeleteRange、Watch、Lease 和 Compact
  的扩展已由后续 P0/A 记录补齐，当前发布判断以本文开头矩阵为准。
- **DeleteRange 双端差分**：新增
  `TestDeleteRangeDifferentialAgainstReferenceEtcd`，覆盖有效范围删除的单
  revision、PrevKVs 顺序和 create/mod/version、删除前历史快照、删除后当前
  快照、`[x,x)` 与缺失点键 no-op。差分发现 standalone 点删缺失键时 backend
  仍调用 TSO 并发布 invalid event，导致每次 no-op 都推进 revision；现已在
  确认 `ErrKeyNotFound` 后直接返回当前 revision，不分配 TSO、不发 watch 事件。
  单元测试重复 20 轮和 race 通过，真实 TiKV 上 DeleteRange/Range/Txn 三组双端
  差分共同连续 10 轮通过。
- **Lease revision 缺口的初始发现（后续三阶段已解决）**：同一差分最初加入 leased key
  后确认 LeaseGrant 的 `leases/<id>` 持久化，以及删除后的
  `leasekeys/<key>` detach，会各自推进 KubeBrain 用户可见 revision；官方 etcd
  的 lease 元数据变更不推进 KV revision。不能简单停止持久化，否则会破坏
  failover 和过期正确性；需设计独立内部元数据存储布局，并让 attachment 与
  对应用户 Put/Delete 在一个存储事务中提交、但只产生用户 KV mutation 的
  revision/watch 事件。该项列为后续 Lease P0，不在本次 DeleteRange 结果中
  隐藏或归一化掉。
- **Lease meta revision 隔离（第一阶段）**：backend 新增按 keyspace 派生的
  raw internal KV 通道，写入持久化到 TiKV，但不分配用户 MVCC revision、不写
  event log、不进入 watch/count index。LeaseGrant 和 lease meta 删除已切换到该
  通道，因此新租约的 Grant/Revoke 不再改变 KV revision。恢复过程同时读取新
  internal 布局和旧用户-MVCC 布局；旧记录迁移成功后才删除，避免 Revoke 后被
  compatibility reader 复活。attachment 尚未切换，leased Put/Delete 仍会多推进
  revision，下一阶段需扩展 TxnApply，使用户 mutation 与 internal attachment 在
  同一 TiKV batch 原子提交、但只为用户 mutation 生成 revision/watch event。
- **Lease attachment revision 隔离（第二阶段）**：`TxnApply` 支持混合 user
  MVCC op 与 internal op；internal op 使用同一 TiKV batch 的 CAS/Put/Delete，
  但不参与 revision、event log、watch 事件和 etcd response。常见 leased Put、
  rebind 和 clear-lease 路径已切换，Grant `+0`、leased Put `+1`、clear lease
  Put `+1` 由回归测试固定。standalone attach/detach 也改走 internal KV，因而
  不再额外推进 revision。恢复同时读取 legacy attachment，并在迁移完整成功后
  退休旧记录。剩余工作是把 generic txn 的 lease binding 和多键
  revoke/expiry attachment 清理也收拢到各自用户写的同一原子 batch。
- **Lease txn/revoke 原子性（第三阶段）**：LeaseRevoke 与 expiry 不再逐 key
  CompareDelete，而是预读 inline lease、为每个 mod_revision 建 guard，并用一次
  `TxnApply` 原子删除全部仍绑定的 key 和 internal attachments；所有删除共享一个
  revision，任一并发 rebind 会使整批回滚重试，失败时 lease/meta 保留。通用 txn
  的 atomic 与 staged 执行器也会在提交前追加 internal attachment op，提交后仅
  更新内存索引；涉及 lease 的单写 Txn 不再回退到非原子顺序路径。普通无 lease
  单写仍保留原快路径。
- **Lease 官方客户端差分**：新增 `TestLeaseDifferentialAgainstReferenceEtcd`，
  对照 `/root/etcd` 的真实 server，覆盖 Grant 不推进 revision、leased Put 单
  revision、TTL/attached keys、KeepAliveOnce、Leases、Revoke 删除 revision、
  revoke 后 key 消失及未知 lease `TTL=-1`。同时修复 Grant/TTL/KeepAlive/Leases
  的空 Header.Revision；真实 TiKV 部署上 Lease/Range/DeleteRange/Txn 四组差分
  共同连续 10 轮通过。
- **Lease 最小 TTL**：对照 etcd lessor 与真实 3.7 server 确认，TTL 小于
  默认 `minLeaseTTL` 时不是报错，而是提升为 2 秒。KubeBrain 现对负数、0、1
  和 2 都返回实际 granted TTL=2，并以该值持久化、调度 expiry；差分矩阵加入
  TTL=0 Grant/Revoke，真实 TiKV 上连续 10 轮通过。
- **Lease 枚举授权对齐（2026-07-16）**：对照
  `/root/etcd/server/etcdserver/v3_server.go` 的 `checkLeaseTimeToLive` 与
  `checkLeaseLeases`，补齐 auth 开启时的 key 级 READ 检查。`TTL(Keys=false)`
  仍只返回租约元数据；`TTL(Keys=true)` 必须能读该 lease 的全部附着 key；
  `Leases` 必须能读所有待枚举 lease 的全部附着 key，否则整次请求返回
  `PermissionDenied`，不泄露受保护 key 名或 lease ownership。完整 server、race
  和 vet 通过。
- **Lease 换主写 fence（2026-07-16）**：Grant/Revoke 与后台 expiry 现在在入场
  时捕获 `(leadership epoch, lease freshness)`，并把 epoch 贯穿 leased-key 的
  `TxnApply`、lease meta 删除和 attachment 清理。确定性回归在 Revoke 通过 leader
  gate 后、TiKV batch 前切换 epoch，验证旧 leader 返回 `Unavailable`，且 leased
  key 与 durable lease meta 均保留；修复前该后台路径因 context 无 epoch 而 fail-open。
  当前源码重建后，真实 TiKV/PD 上 Lease/Txn/Range/DeleteRange/Compact 官方
  client/v3 双端差分共同通过。
- **Lease 自然过期与 DELETE wire 语义（2026-07-16）**：新增官方
  `client/v3` 双端差分，以 TTL=2 的同一 lease 反序绑定 `b`、`a` 两键，从
  `lastPutRevision+1` 订阅 `WithPrevKV`，等待真实 timer 驱动后台 expiry。首次运行
  确认两项差异：KubeBrain 从 map 生成删除 op，watch 顺序随机；DELETE `Kv` 错误
  复制旧对象 create_revision，而 etcd 只在 DELETE `Kv` 保留 key/mod_revision，
  完整旧 value/create/mod/version/lease 仅放 `PrevKv`。现与
  `/root/etcd/server/lease/lessor.go:Revoke` 一致排序 lease keys，显式 Revoke 与
  自然 expiry 均在一个 revision 按 key 升序发布删除；translator 也已对齐 tombstone
  字段。真实 TiKV keyspace 与参考 etcd 连续 10 轮（每轮真实等待两边 expiry）逐字段
  一致：Grant TTL、Put/DELETE 相对 revision、watch header/event 顺序、PrevKv、
  过期后空 Range、TTL=-1 及 Leases 清理全部通过。
- **Watch 控制流与 fragmentation（2026-07-16）**：对照
  `/root/etcd/server/etcdserver/api/v3rpc/watch.go` 与 `storage/mvcc/watcher.go`，
  补齐 stream-scoped 客户端指定 `watch_id`、重复 ID 拒绝、未知 ID cancel 静默
  忽略、显式 cancel 空 `CancelReason`，以及负 `start_revision` 以流内
  `Created+Canceled/WatchId=-1` 响应而不关闭复用 stream。新增 raw gRPC 双向流
  差分，同一控制序列访问真实 TiKV-backed KubeBrain 和参考 etcd，响应逐字段
  一致。`fragment=true` 现按 etcd 默认 `1.5 MiB + 512 KiB overhead` 切分多事件
  response，中间片 `Fragment=true`、末片 false；事件顺序和 header/revision 保持。
  完整 Watch 测试、race、server 回归与 vet 通过。
- **RangeStream 通用语义（2026-07-16）**：对照
  `/root/etcd/server/etcdserver/v3_server.go:rangeStream`、
  `api/v3rpc/key.go:checkRangeStreamRequest` 及 `tests/integration/v3_grpc_test.go`
  补齐 etcd 3.7 的 `CountOnly`、`Limit`、`KeysOnly` 与显式 `ASCEND+KEY`。
  CountOnly/Limit 复用已对齐的 unary Range 并封装为单条流消息；无限扫描继续使用
  固定 revision、按键/字节双阈值分块的低内存路径。修复 scanner 内部 chunk
  `More=true` 泄漏为公开“Limit 截断”语义，以及 terminal `Count=0` 两个 wire
  差异。进一步确认 TiKV 多 Region partition worker 原先竞争共享 channel，会使
  流结果跨 Region 乱序；现各 partition 仍并行预取到容量 1 的有界 channel，协调器
  按排序后的 partition 顺序排空，兼顾升序语义、并行 IO 和背压。确定性测试延迟
  首 partition、让后 partition 先产出 700-key 结果，仍验证全流严格升序；server、
  scanner race/vet 通过。额外以 26 个 partition（超过 24 个全局 worker 配额）、
  每 partition 超过一个满 chunk 覆盖调度死锁边界；worker 按 partition 顺序获取
  配额，避免后区间占满配额后等待协调器、首区间又等待配额的环形等待。官方 3.7
  `client/v3.GetStream` 对独立 TiKV keyspace 与
  `/root/etcd/bin/etcd` 的双端差分连续 10 轮通过，逐字段比较合并后的 KV、
  revision、Count 和 More。
- **Maintenance Hash/HashKV（2026-07-16）**：删除原先仅对 revision 做 CRC 的
  伪校验，改为在逻辑写屏障内扫描指定 revision 之前仍保留的租户对象 MVCC
  版本，并用 CRC32C 生成确定性摘要。event-log、完整性 watermark 和 internal
  service metadata 虽与对象共享物理 tenant 范围，但均被明确排除；首次真实
  TiKV 测试发现并固定了该布局边界。`revision=0` 选择当前 revision，历史摘要
  不受后续写入影响，取消的请求会停止扫描。官方 client/v3 Maintenance 客户端
  已在真实 TiKV/PD 上验证同 revision 稳定、数据变化改变当前摘要、历史摘要
  保持不变。摘要用于比较同一 KubeBrain keyspace 的多个服务端；由于 etcd 对
  bbolt KV bucket 的内部编码求 hash，而 KubeBrain 使用 TiKV 对象编码，两者的
  数值本身不具有跨引擎可比性。
  三副本真实 TiKV/PD 验证进一步由 MemberList 枚举每个 ClientURL，在一次 Put 的
  固定 revision 上逐 endpoint 调用官方 clientv3 `HashKV`：三个 hash 与 ClusterID
  完全相同，三个响应 MemberID 各不相同且对应实际服务副本。
- **ClusterId 稳定性（2026-07-16）**：`MemberList` 不再把当前 leader 地址的
  CRC 当作 ClusterId，改为与所有其他 RPC 一致地使用 backend 从 PD/TiKV
  cluster identity 和 keyspace 派生的稳定 ID，避免换主时客户端把同一实例误判
  为另一集群。当前 peer service 只掌握本机与 leader，尚不能枚举全部 follower；
  因此 MemberList 仍是部分兼容，DBaaS 需要把实例副本注册表注入数据面后再宣称
  支持 clientv3 AutoSync。
- **Maintenance 授权矩阵（2026-07-16）**：对齐
  `/root/etcd/server/etcdserver/api/v3rpc/maintenance.go` 的
  `authMaintenanceServer`：auth 开启后，`Status` 与 `Alarm(GET)` 要求任意有效
  身份；`Hash/HashKV`、`Defragment`、`Snapshot`、`MoveLeader`、`Downgrade`
  以及 Alarm 状态变更要求 root。鉴权发生在 SyncRead、全 keyspace hash 和平台
  `Unimplemented` 返回之前，普通用户不能触发昂贵维护读，也不能借错误差异探测
  管理操作。匿名、普通用户、root 三类回归及 race 已覆盖。
- **MemberList/AutoSync（2026-07-16）**：新增与 etcd 同形的
  `--initial-cluster=name=http[s]://host:peerPort,...`，由 DBaaS 控制面向每个
  KubeBrain 副本注入相同服务成员配置。启动时严格校验 URL、名称、peer identity
  唯一性，并要求包含本机 `--advertise-host:peer-port`；MemberList 返回完整集合，
  ClientURLs 按统一 client port/TLS 模式派生，ClusterId 仍来自 PD/TiKV storage
  identity，MemberId 对应实际应答副本。未配置时保留本机+leader 降级视图。官方
  clientv3 `Sync` 回归验证其 endpoint 集合与 MemberList 完全一致。
  真实验证使用隔离容器网络启动 3 个同端口副本（`172.31.250.11-13:2379`）并
  共享现有 TiKV/PD：三个 endpoint 均返回同一 3-member 集合和 ClusterID，各自
  MemberID 对应实际应答副本。停止初始 leader `.11` 后，`.12` 在租约窗口后接任；
  clientv3 从 `.12` Sync 后仍保留包含离线 `.11` 的静态三端点集合，随后 Put/Get
  成功，证明 resolver 会选择 ready 副本而不会因配置成员暂时离线失效。
  继续对齐 `/root/etcd/server/etcdserver/server.go:MemberList`：auth 开启后任意
  MemberList 都要求有效身份；`Linearizable=true` 会先执行 revision read barrier，
  barrier 失败不返回可能陈旧的成员视图。MemberAdd/Remove/Update/Promote 虽由
  DBaaS 控制面替代并返回 Unimplemented，仍先要求 root，避免管理面 auth 绕过。
- **Status DbSize 官方工具兼容（2026-07-16）**：真实三 endpoint 验证发现
  `/root/etcd` 3.7 的 `etcdctl endpoint status -w table` 会直接计算
  `DbSizeInUse*100/DbSize`，原有 0/0 使官方工具除零 panic。现返回相等的 1 字节
  sentinel，表达“无 etcd bbolt fragmentation 可报告”，不伪装 TiKV 容量；真实
  磁盘、region 和 quota 继续由 TiKV/PD/DBaaS 指标提供。
  修复镜像在上述三个 endpoint 上执行官方
  `etcdctl endpoint status -w table` 已正常显示 `1 B / 1 B / 0%`，不再 panic。
- **Status leader/版本诊断（2026-07-16）**：修复无 leader 时把未知地址回退为
  本机 ID、导致每个副本都声称自己是 leader 的错误；现在与 etcd 一致返回
  `Leader=0` 并在 Errors 中加入 `etcdserver: no leader`。同时显式返回
  `StorageVersion=3.7.0`（表示 KubeBrain 当前对外持久化语义版本，不表示 bbolt
  格式）和 `DowngradeInfo{Enabled:false}`，避免官方诊断输出留空或依赖 nil 默认。
- **Alarm 平台边界（2026-07-16）**：`Alarm(GET)` 继续返回空集合，因为 TiKV
  没有 KubeBrain 单逻辑库的 bbolt NOSPACE/CORRUPT alarm；`ACTIVATE/DEACTIVATE`
  不再“成功但不保存、不执行写保护”，而是在 root 鉴权后明确返回
  `Unimplemented`。容量、磁盘与数据完整性告警及处置由独立 TiKV/PD 集群和 DBaaS
  控制面负责，避免 etcd 客户端误以为 alarm 已生效。
- **Compact 双端差分**：新增 `TestCompactDifferentialAgainstReferenceEtcd`，
  覆盖 logical compaction 成功 header、`revision == compactRev` 边界快照仍可读、
  `revision < compactRev` 返回 ErrCompacted、重复/更旧 compact 返回同一错误、未来
  revision 返回 ErrFutureRev，以及当前值不受影响。真实 TiKV 与 etcd 3.7 连续
  10 轮结果一致。差分不再要求 revision 连续 `+1`：etcd 只为成功提交的写事务
  分配提交序号；KubeBrain 的 leader-local TSO 虽按 `+1` 分配，但并发写会在存储
  提交前预留 revision，失败、CAS 冲突和 uncertain retry 可留下无事件的跳号。
  revision 仍全局唯一且严格递增。这是通用 DBaaS 的已知可观察差异，后续需用
  真实客户端兼容矩阵判断是否必须重构为提交时连续编号，不能在语义测试里误报
  或掩盖。
- **Compact 管理权限（2026-07-16）**：对照
  `/root/etcd/server/etcdserver/api/v3rpc/key.go` 的 `AuthAdmin.isPermitted`，
  Compact 在 Auth enabled 时改为仅 root 可执行。匿名请求返回 `ErrUserEmpty`，
  已认证普通用户返回 `ErrPermissionDenied`；follower 代理会转发认证 token，并由
  leader 再次校验，避免通过任一副本绕过权限。官方 `client/v3` 对独立 etcd 3.7
  和真实 TiKV-backed KubeBrain 执行匿名、普通用户及 root 三组请求，状态码、错误
  文本和成功结果一致。
- **流式与 Maintenance 鉴权差分（2026-07-16）**：官方 `client/v3` 双端
  Auth 生命周期新增 RangeStream、Status 和 HashKV。RangeStream 对匿名及无范围
  READ 权限用户分别拒绝，root 可完整读取 3 个对象；Status 允许任意已认证用户但
  拒绝匿名请求；HashKV 仅允许 root。server-streaming 的错误从异步接收通道取得后，
  gRPC code/message 仍与参考 etcd 一致。静态复核同时确认 MemberList 要求已认证、
  MemberAdd/Remove/Update/Promote 先要求 root 再返回平台替代的 Unimplemented。
- **LeaseKeepAlive 绑定键鉴权（2026-07-16）**：对照
  `/root/etcd/server/etcdserver/v3_server.go:checkLeaseRenew`，KeepAlive 不再只在
  双向流建立时确认 token，而是对每个请求重新恢复 caller，并要求其对该 lease
  当前附着的全部 key 拥有 WRITE 权限。修复前，知道其他用户 lease ID 的已认证
  调用者可以持续续租无权写入的键，阻止其按 TTL 过期；follower 转发也会把请求
  送到 leader 重新按真实 attachment 集合校验。确定性测试覆盖同一长连接首个
  renew 后撤销权限、第二个 renew 立即拒绝；官方 `client/v3 KeepAliveOnce` 在独立
  etcd 与真实 TiKV-backed KubeBrain 上均表现为普通用户 `PermissionDenied`、root
  成功。
- **Range auth revision 序列化（2026-07-16）**：对照 etcd
  `EtcdServer.doSerialize`，认证 caller 的 revision 改为请求开始时加载的持久化 auth
  revision；unary Range 和原生 RangeStream 在读取结束时重新点读 revision。若角色或
  权限在读取期间变化，响应以 `ErrAuthOldRevision` 失败，避免客户端把旧权限下的
  Range 结果或部分 stream 当作完整成功；请求开始前已经发生的无关 auth mutation
  不会废掉仍有效的 simple token。确定性 backend hook 覆盖点读完成后撤权，以及
  RangeStream 首 chunk 发出后撤权；下一次请求按新权限返回 PermissionDenied。
  只读通用 Txn 不额外套此检查：其 staged executor 已持有逻辑事务屏障，auth 的
  InternalCAS 在事务结束前无法提交；源码锁序和确定性注入时的阻塞栈确认不存在
  同一窗口。
- **Physical compaction 故障恢复（2026-07-16）**：真实 TiKV 灌入 500 key ×
  20 versions 后验证 Physical=true 在扫描完成后才返回；100ms 客户端取消时逻辑
  水位已经单调推进、旧 revision 返回 ErrCompacted、当前值可读。修复了取消/进程
  退出后无自动追赶，以及后台扫描失败仍错误推进 `compactDoneRev` 的问题：物理
  扫描现返回错误，失败不记完成并每秒有界重试；新 Leader 在 SERVING 前读取持久
  化逻辑水位并调度恢复。实测重启后自动扫描目标水位；TiKV 停止期间新 Pod 保持
  0/1，恢复后自动完成扫描。新增 `TestPhysicalCompactionUnderTraffic`，用官方
  client/v3 在 Physical compact 同时执行 100 次 Put/Get、前缀 Range 和 Watch；
  KubeBrain+TiKV 与参考 etcd 均通过，100/100 watch 事件完整，历史边界一致。
- **官方 concurrency recipes（2026-07-16）**：新增 endpoint 驱动的
  `TestConcurrencyMutexAndSessionRelease` 与
  `TestConcurrencyElectionObserveProclaimAndHandoff`，覆盖 Mutex Lock/TryLock、
  owner session Close 后 lease 自动释放、Election Observe/Proclaim、竞争者阻塞、
  Resign 后接棒。KubeBrain+TiKV 和 `/root/etcd` 参考 server 各连续 3 轮通过。
  opt-in `TestConcurrencySessionSurvivesKubeBrainFailover` 在 KubeBrain 3 副本上
  持有 Mutex+Election 后删除当前 Leader，约 6.25 秒恢复；原 session 未关闭、
  Mutex ownership 和 Election leader 值均保持，随后正常 Resign/Unlock。由此确认
  官方 concurrency 中使用 `header.revision+1` 表示 watch 下界，不要求每个中间
  revision 都实际存在；KubeBrain 的失败预分配跳号未破坏这些 recipe。
- **Concurrency A35 orphan session 自然过期（2026-07-16）**：新增
  `TestConcurrencyOrphanedSessionExpiresAndHandsOff`，同一 TTL=2s owner session 同时
  持有 Mutex 与 Election，`Session.Orphan` 只停 keepalive、不主动 Revoke；确认
  竞争者在 orphan 后仍阻塞，lease 自然到期后两者都接棒，旧 lease TTL=-1
  且新 Election leader 值正确。参考 etcd 用时 2.30s，当前三副本
  KubeBrain+TiKV 用时 2.22s，行为与时序一致。
- **一致性 A36 Porcupine 操作历史（2026-07-16）**：兼容子模块引入与
  etcd robustness 测试相同的 `github.com/anishathalye/porcupine`，新增单键 register
  模型和 `TestClientV3RegisterHistoryIsLinearizable`。每轮 5 个独立 client 并发执行
  60 个线性 Get、Put 和 compare-and-swap Txn，以 RPC 调用/返回时序构建
  history 并要求 checker 返回 `Ok`；反向单测传入“Put 完成后 Get 仍读旧值”
  的不可能历史，确认 checker 返回 `Illegal`而非形同虚设。参考 etcd
  连续 5 轮、当前三副本 KubeBrain+TiKV 连续 10 轮通过。本轮只对成功
  RPC 的无故障历史作确定性判定；故障期间超时/`Unavailable` 写可能已落库，
  不能不加证明地丢弃，下一步需对齐 etcd nondeterministic model 后再做 Leader/
  TiKV 故障历史校验。
- **一致性 A37 不确定写与 Leader 故障历史（2026-07-16）**：将 register
  checker 升级为 Porcupine `NondeterministicModel`。失败 Get 不改变状态；失败
  Put 分叉为未落库/已落库；失败 CAS 只在当时值匹配 predicate 时分叉为
  未落库/已更新，后续成功 read/CAS 会剪枝不可能状态。反向单测确认
  失败 Put 与 predicate 成立的失败 CAS 两种结果都合法，predicate 不成立的
  CAS 不能凭空改值，已完成 Put 后读旧值仍为 `Illegal`。故障模式在
  5 client、150 操作重叠期间删除当前 KubeBrain Leader，两轮连续删除新
  Leader 均捕获 2 个 `Unavailable` 不确定写，完整 history 均返回 `Ok`；
  参考 etcd 的无故障基线在模型升级后连续 5 轮通过。下一步将模型扩展到
  多键 Txn/lease，并注入 TiKV/PD 层故障。
- **一致性 A38 多键 Txn 原子历史（2026-07-16）**：新增两键 pair 的
  Porcupine nondeterministic model 与
  `TestClientV3MultiKeyTxnHistoryIsLinearizable`，通过官方 client/v3 Txn 并发
  执行同 revision 的两键 Get、两键 Put 和同时比较/更新两键的 CAS。每次
  更新写入 `(n,-n)`；失败 Put/CAS 仅分叉为“整笔未落库”或“整笔落库”，
  模型反向测试确认成功或失败 Txn 后都不可观测 `(new,old)` 撤裂状态。
  参考 etcd 5 轮、当前三副本 KubeBrain+TiKV 10 轮无故障基线均通过；
  KubeBrain 当前 Leader 删除期间的 5 client/150 操作 history 捕获 3 个
  `Unavailable` 不确定 Txn，完整 history 仍为 `Ok`。下一步扩展 lease
  生命周期模型并注入 TiKV/PD 层故障。
- **一致性 A39 TiKV/PD 故障历史（2026-07-16）**：将同一多键 Txn
  checker 的故障注入扩展到可配置的 `LINEARIZABILITY_DELETE_NAMESPACE`/
  `LINEARIZABILITY_DELETE_POD`，不再只能删除 KubeBrain 副本。在三副本
  KubeBrain 连接独立单 store TiKV/单 PD 的当前 dev 集群上，分别在
  5 client/150 两键 Txn 操作期间删除 `tidb-cluster/kb-tikv-0` 和
  `tidb-cluster/kb-pd-0`。TiKV 轮捕获 1 个不确定 Txn，10.60s 恢复并返回
  `Ok`；PD 轮覆盖 TSO `server not started`、EOF 和 proxy not-ready，捕获
  11 个不确定 Txn，11.54s 恢复并返回 `Ok`。两轮后 Pod 均重建
  Ready，后续成功读对可能落库的失败 Txn 状态完成剪枝，未观测到部分提交。
  下一步扩展 lease 生命周期模型，并在多 store/多 PD 预生产拓扑上执行
  分区、leader 转移和多点故障。
- **一致性 A40 Lease 生命周期历史与 revoke 竞态修复（2026-07-16）**：新增
  `TestClientV3LeaseLifecycleHistoryIsLinearizable` 和 Porcupine
  nondeterministic model，以 5 个独立 client 并发执行 lease-bound Put、Get、
  KeepAliveOnce 和 Revoke；失败 Put/Revoke 按可能落库分叉，确定性的
  lease-not-found 则只允许出现在 lease 已死亡状态。参考 etcd 连续 5 轮通过；
  首次 KubeBrain 基线稳定复现“Revoke 已返回且 KeepAlive 报 not-found 后，
  并发 Put 才提交并复活旧 lease key”的 `Illegal` history。根因是 lease 存在性
  校验与 value+attachment 原子 batch 之间没有和 revoke 排序。新增独立
  `leaseWriteMu`：Put/Txn 从校验到提交及索引更新持读锁，显式 Revoke/TTL expiry
  从 key 删除到 lease 移除持写锁；确定性阻塞 backend 的单测确认已准入 Put
  必须先提交，随后 Revoke 删除该 key。修复镜像在三副本 KubeBrain+独立 TiKV/PD
  上无故障连续 10 轮返回 `Ok`；删除当前 KubeBrain Leader 捕获 1 个不确定 RPC，
  删除单 TiKV 时短暂故障被 client 重试完全屏蔽，删除单 PD 时捕获 3 个不确定
  RPC，三类完整 history 均返回 `Ok`。故障模式 TTL 提高到 300s，避免 90s
  故障窗口中的合法自然过期超出本模型范围。
- **读取 A41 serializable follower 可用性（2026-07-16）**：对照
  `/root/etcd/server/etcdserver/v3_server.go`，`RangeRequest.Serializable=true`
  不应执行 leader read-index。KubeBrain 的 revision-zero unary Range 与
  RangeStream 现跳过 `SyncReadRevision`，可直接从共享 TiKV snapshot 服务；普通
  linearizable 请求仍 fail-closed。新增注入失败 revision syncer 的单测，并用
  client/v3 差分覆盖 current/historical serializable Range 及 compare+read Txn，
  参考 etcd 与 TiKV-backed KubeBrain 连续 10 轮一致。真实三副本测试直连 follower、
  删除当前 Leader 后连续执行 30 次 500ms-bound serializable Get，1.98s 内零错误。
  **A41 当时的保留边界**：显式 historical revision 仍需 proxy/sync；KubeBrain follower 不像
  Raft member 持续 apply user revision，仅共享 TiKV 数据，不能把任意 client revision
  当作已应用水位。实验同时发现 read-only Txn 若直接使用 follower-local
  `GetCurrentRevision`，compare 可见新值而 staged branch snapshot 仍读旧 revision，
  产生撕裂响应；该路径未合入，Txn 继续 leader-fenced。完整对齐需先增加集群级、
  单调且仅代表连续已提交 user revision 的 durable watermark，再让 compare 与 branch
  固定到同一 snapshot；不能用 fresh PD TSO 伪装 header，否则会越过后续用户 revision。
  该边界已由下述 A42 的 durable user revision 水位解除。
- **读取 A42 durable user revision 水位（2026-07-16）**：实现 tenant-scoped
  `revision/committed` 持久化水位。leader event collector 仅在从队头到目标 revision
  的所有 deal slot 均已解析后推进水位；成功写此时已落 TiKV，失败/放弃 slot 不含
  user state。水位通过精确旧值 CAS 单调推进，marker 写失败只导致安全滞后，后续
  revision 会重试，不会向 follower 暴露尚未提交的快照。read-only Txn 严格复用
  upstream `IsTxnReadonly`/`IsTxnSerializable` 判定：仅当 success/failure 顶层操作
  全部为 `Serializable=true` Range 时绕过 leader；compare 与选中 branch 固定到同一
  durable revision，LEASE compare 也读取该 MVCC 版本内联 lease。显式 historical
  unary Range/RangeStream 仅在 `requestedRevision <= durableRevision` 时本地服务，
  否则保留 proxy/sync fail-closed。marker 缺失（升级后尚无新 user revision）同样
  回退 leader，不把 fresh PD TSO 当成用户 header。定向单测覆盖 stale follower
  current revision 下 compare/branch 不撕裂、水位不回退、historical stream 不触发
  revision sync；clientv3 failover 场景扩展为删除 leader 后循环验证 current Range、
  historical Range 与 serializable compare+read Txn。
- **Auth A1 持久化基础（2026-07-16）**：新增原子 `Backend.InternalCAS`，先读并
  校验全部 internal key 的精确旧值，再打开一个 storage batch 统一 CAS/创建/删除；
  冲突返回 `ErrCASFailed` 且整批不落地，不消耗用户 revision。etcd shim 已贯通该
  原语。新增 auth repository，以 `auth/config` 的独立 revision 串行化
  `auth/users/*` 与 `auth/roles/*` protobuf 记录，支持全量恢复和确定性编码；旧
  revision mutation 不会部分覆盖对象。bootstrap manager 已实现但**尚未挂公开
  RPC**：UserAdd、RoleAdd、UserGrantRole、AuthEnable 前置条件与幂等行为，密码
  使用 bcrypt；缺 root user/root role 返回 etcd 对应错误。定向普通/race 与
  backend/server 全量测试通过。下一步补齐 CRUD/permission，再做 token 和数据面；
  在此之前公开 Auth 仍保持 Unimplemented。
- **Auth A2 管理模型（2026-07-16）**：private auth manager 已补 UserDelete、
  UserChangePassword、UserRevokeRole、RoleDelete、RoleGrantPermission、
  RoleRevokePermission 和 AuthDisable。角色删除与所有受影响用户解绑通过同一个
  internal CAS batch 和同一个 auth revision 提交；权限按 `(key, range_end)` 精确
  更新/撤销并保持 key 排序，开放尾区间 `{0}` 与非法反向区间规则对齐 etcd。
  Auth 开启后禁止删除 root user/role 或撤销 root 绑定，关闭后允许清理。完整
  lifecycle、错误、root 保护、普通/race 和全量回归均通过。该 manager 仍未挂到
  gRPC；下一步先做多副本快照同步和 token，再一次性开放管理 RPC 与数据面保护。
- **Auth A3 令牌基础（2026-07-16）**：新增 private token manager。首次认证通过
  `InternalCAS` 生成并持久化 256-bit HMAC-SHA256 签名密钥，多副本和进程重启
  后可验证同一令牌；令牌包含用户名、auth revision、签发/过期时间和随机 nonce，
  默认 TTL 5 分钟。密码认证使用持久化 bcrypt hash；任何 auth mutation 推进
  auth revision，旧令牌立即失效。校验覆盖签名、过期/未来时间、revision、用户
  存在性和 AuthEnable 状态；密钥缺失或损坏时失败关闭，验证路径不会隐式生成或
  轮换密钥。认证快照以持久化 config revision 作跨副本失效信号：稳态请求只点读
  config，revision 变化时单飞重载 users/roles；重载前后双读 revision，mutation
  穿越扫描窗口时整轮重试，不发布撕裂权限快照。该层仍为 private，尚未开放
  AuthAuthenticate 或保护 KV/Watch；下一步接入公开管理 RPC、认证 RPC 和 unary
  数据面权限检查，最后单独处理长连接 Watch 的持续鉴权。
- **Auth A4 安全公开面（2026-07-16）**：RPCServer 已显式接入 AuthStatus、
  Authenticate 以及 User/Role bootstrap CRUD，响应使用官方 etcd protobuf，root
  RoleGet 返回隐式全 keyspace READWRITE 权限。为避免半成品造成认证绕过，bootstrap
  CRUD 仅在 Auth disabled 时开放；检测到持久化 Enabled 状态后，未鉴权管理请求
  统一拒绝。AuthEnable/AuthDisable 仍保持 Unimplemented，因此生产入口暂时无法把
  实例切入“已启用但 KV/Watch 未保护”的危险状态。单测覆盖 disabled bootstrap、
  status、root 权限、模拟已启用后的 token 签发和管理面失败关闭。下一步实现统一
  caller 提取与 unary KV/Lease/Auth 管理鉴权，通过后才开放 AuthEnable/Disable。
- **Auth A5 独立鉴权器（2026-07-16）**：新增从 gRPC incoming metadata 的官方
  token 字段恢复 caller；Auth disabled 时透明放行，Enabled 时缺 token、无效签名、
  过期或旧 auth revision 全部失败关闭。key-range 权限按 etcd 半开区间语义实现，
  支持单 key、`{0}` 开放尾区间、READ/WRITE/READWRITE、root 快路径，并会归并同一
  用户跨多个角色的相邻权限，因此既不会误拒合法组合区间，也不会跨权限间隙放行。
  该鉴权器尚未挂 KV handler；下一步逐项接 Range/Put/DeleteRange/Txn，并针对嵌套
  Txn 的 compare、success/failure 两分支做全树预检，保证写入前拒绝而非部分执行。
- **Auth A6 KV 数据面（2026-07-16）**：鉴权已接入 Range、RangeStream、Put、
  DeleteRange 和 Txn，并位于 leader/proxy/backend 操作之前。Put/Delete 的 PrevKV
  额外要求 READ；Put 绑定已有 lease 时要求对该 lease 全部绑定键具备 WRITE，防止
  借共享 lease 间接删除越权键。Txn 按 etcd `CheckTxnAuth` 语义递归预检全部 compare、
  success 和 failure 分支，包括嵌套 Txn；任何拒绝都发生在执行前，不产生部分写。
  follower 代理只把官方 token metadata 转发给 leader，leader 会重新验证签名和
  auth revision，不信任 follower 判定。handler 测试确认允许范围可读写、越权 Put
  不落库、PrevKV 权限失败不删除、未选/嵌套分支越权会整笔拒绝。下一步接 Lease
  RPC 和 Auth 管理面 admin/self 规则，然后才能安全开放 AuthEnable。
- **Auth A7 Lease 数据面（2026-07-16）**：LeaseGrant、LeaseTimeToLive、
  LeaseLeases 和 KeepAlive 均在 Enabled 状态要求有效 caller；LeaseRevoke 进一步要求
  caller 对该 lease 全部绑定键具备 WRITE，与 etcd `checkLeasePuts` 对齐，因此不能
  通过撤销共享 lease 间接删除越权键。KeepAlive 在流建立时鉴权；所有 follower
  lease 代理携带 token 到 leader 重新验证。测试确认缺 token Grant 被拒绝、越权
  Revoke 不删除绑定键、合法 caller 的 Grant/TTL/List 正常。下一步完成 Auth 管理
  RPC 的 root admin 与 UserGet/RoleGet self 例外，再开放 Enable/Disable。
- **Auth A8 管理面鉴权（2026-07-16）**：Enabled 状态下 User/Role mutation、
  UserList 和 RoleList 仅允许 root；UserGet 允许 root 或用户本人，RoleGet 允许 root
  或已绑定该角色的用户，与 etcd auth applier 例外规则一致。AuthStatus 在 Enabled
  状态要求有效 token，但不要求 root。Disabled 状态仍允许无 token bootstrap。
  所有 mutation 推进 auth revision，包含执行 mutation 的 root 在内的旧 token 都
  立即失效；测试显式重新 Authenticate 后继续管理。普通用户越权查询、列表和角色
  创建均被拒绝。下一步完成 Watch 建流与运行期 revision 变化处理，然后开放
  AuthEnable/AuthDisable 并跑官方 client/v3 黑盒生命周期。
- **Auth A9 Watch 与启停闭环（2026-07-16）**：每个 WatchCreateRequest 都从
  gRPC stream metadata 验证 token 和 READ range；失败时按 etcd 返回
  `Created=true,Canceled=true,WatchId=-1` 及标准 cancel reason，但不关闭复用流，
  后续合法 create 仍可成功。严格对齐 etcd：已建立 watch 不因后续 auth revision
  mutation 被服务端主动取消，新 create 会因旧 token 失败。至此 KV、RangeStream、
  Watch、Lease 和 Auth 管理面均受保护，AuthEnable/AuthDisable 已公开：首次 Enable
  依赖 disabled bootstrap 的 root user+role；Enabled 后重复 Enable/Disable 均要求
  root token。测试覆盖缺 root、缺 token Disable、root Disable 和关闭后匿名状态。
  下一步在独立 TiKV keyspace/实例上运行官方 client/v3 auth 生命周期黑盒，不能在
  当前 Kubernetes 共用后端直接 Enable 以免切断 apiserver。
- **Auth A10 官方客户端真实 TiKV 黑盒（2026-07-16）**：新增 opt-in
  `TestAuthLifecycle`，只接受显式 `KUBEBRAIN_AUTH_TEST_ENDPOINT`，防止误在 apiserver
  后端启用认证。使用独立 `auth-e2e-3` keyspace、真实 PD/TiKV 和官方 client/v3
  完整通过：root/alice bootstrap、Enable、匿名拒绝、允许/拒绝 Put+Get、授权/越权
  Watch、Lease Grant/绑定/Revoke、root mutation 后 client 自动重新 Authenticate、
  Disable 后匿名写恢复。黑盒发现并修复内部 rpctypes error 直接穿透时被 gRPC 编码
  为 Unknown：现由统一 unary/stream interceptor 映射到 etcd `ErrGRPC*`，实测匿名
  为 InvalidArgument、越权为 PermissionDenied、旧 token 为 Unauthenticated。该问题
  直接 handler 单测无法发现，证明官方消费端黑盒必须保留。下一步补多副本 token
  跨 Pod 验证、leader failover 和 Enabled 状态重启恢复，再扩大 auth 差分矩阵。
- **Auth A11 三副本 HA（2026-07-16）**：新增 opt-in
  `TestAuthTokenSharedAcrossEndpoints`、`TestAuthTokenSurvivesLeaderFailover` 和
  `TestAuthTokenSurvivesEnabledRollout`。真实 3 副本 KubeBrain + 独立 `auth-ha-1`
  keyspace 上：仅向 Pod A Authenticate 一次所得 token 可直接访问 A/B/C；删除当前
  leader 后不重新认证，原 token 在约 10.4s 选主窗口后于两个存活副本恢复写读；
  Enabled 状态滚动替换全部 3 Pod 后，重启前 token 仍可读数据且 AuthStatus=true，
  稳定 NodePort 下完整恢复 23.46s。由此验证 HMAC key、auth config/users/roles 与
  数据均由 TiKV 持久化，多副本不依赖进程本地认证状态。测试显式要求 disposable
  namespace、endpoint 和 leader pod 环境变量，默认跳过 destructive failover。
  下一步对照参考 etcd 扩展 auth 错误/幂等/密码和 permission 边界差分，并测试
  多副本并发 Auth mutation 的 CAS 串行化。
- **Auth A12 匿名状态查询对齐（2026-07-16）**：使用 `/root/etcd/bin/etcd`
  与官方 `etcdctl` 实测确认，认证启用后 `AuthStatus` 仍是公开 RPC，可匿名读取
  `Enabled` 和 `AuthRevision`；KubeBrain 原实现错误地要求 token。现已移除该鉴权，
  handler 回归测试与官方 `client/v3` 生命周期黑盒均覆盖启用后的匿名查询。
- **Auth A13 无密码用户错误对齐（2026-07-16）**：真实 etcd 3.7 实测确认，
  `NoPassword` 用户尝试认证返回 gRPC `Unknown`，文本为 `auth: authentication
  failed, password was given for no password user`，而普通错误凭据返回
  `InvalidArgument/AuthFailed`。KubeBrain 现已拆分两条路径；非法 base64
  `hashed_password` 也与 etcd store 一样归入前者。内部、gRPC 和官方客户端三层
  测试固定该兼容行为。
- **Auth A14 内建 root 角色对齐（2026-07-16）**：etcd 的标准 bootstrap 是
  `UserAdd(root)`、`UserGrantRole(root, root)`、`AuthEnable`，无需先创建 root 角色
  对象。对照 `/root/etcd/server/auth/store.go` 与真实 etcd 后，KubeBrain 现仅对
  特殊角色名 `root` 绕过角色存在检查，启用时只要求 root 用户持有该角色；普通
  角色仍必须先创建。官方 `client/v3` 生命周期已改为标准三步流程。
- **Auth A15 密码字段优先级（2026-07-16）**：对照 etcd `v3_server.go` 的
  `UserAdd`/`UserChangePassword` 预处理，若请求同时携带非空 `password` 与
  `hashed_password`，必须以明文重新 bcrypt 并覆盖 hash；仅明文为空时才接受
  base64 hash。KubeBrain 原先顺序相反，现已对齐并用非法 hash + 有效明文回归。
- **Auth A16 启停 revision 语义（2026-07-16）**：对照 etcd `AuthEnable` 与
  `AuthDisable` 实现，启用只持久化 enabled 标记、不推进 auth revision；禁用则通过
  `commitRevision` 推进一次。KubeBrain 原先启用也加一，导致 AuthStatus 与 token
  revision 偏移，现已拆分持久化路径，并回归验证 enable 保持、disable +1。
- **Auth A17 双端点自动差分（2026-07-16）**：新增 opt-in
  `TestAuthDifferentialAgainstEtcd`，要求显式提供两个空的 disposable endpoint，
  使用同一套官方 `client/v3` 请求分别驱动参考 etcd 与 KubeBrain。首批矩阵覆盖
  内建 root bootstrap、启用 revision、重复授权/角色创建、隐式 root RoleGet、错误
  凭据、NoPassword 和撤销未授予角色，并逐项比较 gRPC code、文本和 revision。
- **Auth A18 初始 revision 对齐（2026-07-16）**：首次真实双端点差分发现所有
  error code/text 一致，但 KubeBrain revision 恒比 etcd 小 1。根因是 etcd
  `NewAuthStore` 会在空 backend 上先提交初始 revision=1；KubeBrain 原逻辑值为 0。
  现将缺失配置记录解释为 revision 1，第一次 mutation 仍以 CAS-not-exists 原子创建，
  后续 revision 与 etcd 完全同序。
- **Auth A19 并发 CAS 饥饿（2026-07-16）**：64 个不同 auth mutation 同时执行的
  `-race -count=10` 压力测试稳定复现固定 16 次重试耗尽并返回 `Unavailable`。现将
  mutation 与一致快照加载都改为退避重试、由 RPC context/deadline 控制退出；修复
  后全部 mutation 成功且 revision 连续，同名并发创建仍恰好一次提交。真实
  etcd/KubeBrain 双端点并发矩阵进一步验证：16 个不同 RoleAdd 全成功并推进 16，
  16 个同名 RoleAdd 均为 1 成功 + 15 AlreadyExists，且仅推进 1 个 revision。
- **Auth A20 failover 并发矩阵（2026-07-16）**：新增 opt-in
  `TestConcurrentAuthMutationsSurviveLeaderFailover`，在删除当前 leader 的同时提交
  32 个唯一 RoleAdd；未知提交结果按幂等语义重试，恢复后要求 32 个角色全部存在且
  auth revision 恰好 +32。测试只接受显式 disposable namespace 和稳定 Service
  endpoint，避免 port-forward 随 Pod 退出造成假失败。真实 3 副本 + TiKV/PD
  实测删除当前 leader 后 32/32 mutation 全部落库，revision 从 3 精确到 35，角色
  列表无遗漏，新 leader 与替换 Pod 均正常且所有副本 0 重启。
- **Auth A21 生命周期边界差分（2026-07-16）**：双端点矩阵扩展同 range permission
  替换、撤销不存在 permission、非法 range、角色删除与用户解绑、enabled 状态下
  root 用户/角色保护、auth mutation 后旧 token 失效，以及改密码后旧密码失败与
  新密码成功。首次实测发现 KubeBrain 错误地在任意 auth mutation 后全局失效
  token，而 etcd simple token 仅在对应用户删除/改密码时失效。现以持久化的每用户
  随机 generation 实现同语义：用户新增/改密码原子创建或轮换，删除时原子移除，
  角色/权限变化保持不变；旧用户首次登录以 CAS 懒迁移且不推进 auth revision，旧版
  revision token 在滚动升级期间仍可验证。修复后完整双端点矩阵通过。
- **Auth A22 follower 转发故障恢复（2026-07-16）**：真实 Docker 三副本开启
  auth 后停止 leader，发现新 leader 可写，但另一个 follower 持续拒绝转发。根因是
  follower 用无凭据 `MemberList` 探测内部 leader 连接，而上游 etcd 与 KubeBrain
  在 auth enabled 时都要求该 RPC 携带身份；健康连接因此被误判为不可用。现改为
  检查底层 gRPC/HTTP2 transport Ready，用户请求的 token metadata 仍原样转发，
  不放松公开 `MemberList` 鉴权。transport 单测、race/vet 通过；同一 TiKV/PD
  keyspace 上重建三副本、停止当前 leader 后，预故障 token 在两个存活端点均恢复
  Put/Get，且直接 `etcdctl` 经 follower 写入可由新 leader 读取。
- **Auth A23 写侧 revision 原子隔离（2026-07-16）**：对齐 etcd 将请求携带的
  `AuthRevision` 纳入 apply 的语义。认证后的 Put/Delete/Txn 和 LeaseRevoke 现在把
  当前 auth config 作为 no-op CAS guard，和 Create/Update/Delete/DeleteRange/
  `TxnApply` 的业务键写入放在同一个存储事务中；授权后若角色或权限变化，整个批次
  原子失败并返回 `ErrAuthOldRevision`，不会出现已撤权请求在另一副本继续提交的
  TOCTOU 窗口。后端测试覆盖全部写形状、成功 guard 不改写配置以及冲突无部分写；
  RPC 测试在鉴权后、提交前确定性撤权；真实 TiKV BatchWrite 合约验证 guard 冲突时
  sibling 写不落盘。测试键已按运行和子测试隔离，避免持久 TiKV 上残留数据造成
  “缺失键 CAS”假通过或假失败。
- **Auth A24 客户端证书 CN 身份（2026-07-16）**：对齐 etcd
  `AuthInfoFromTLS`：仅在 client endpoint 启用 `--client-cert-auth` 且 TLS verified
  chain 存在时，把叶证书 CommonName 作为用户名；显式 token 优先，无效 token 不得
  回退证书；带 `grpcgateway-accept` 的代理请求禁止借服务端证书 CN 冒充终端用户。
  KubeBrain follower 不能像 etcd Raft apply 一样天然保留入口 TLS context，因此会在
  本地验证证书后，用 TiKV 持久化、全副本共享的 HMAC key 签发 5 分钟证书身份 token
  转发给 leader；leader 验签后按当前 RBAC 重新授权，未知 CN 仍是有效传输身份但没有
  键权限。测试覆盖配置贯穿、token/CN 优先级、gateway 防护、未知 CN 以及 follower
  代理后 username 不变，防止 leader 错把 follower 自身证书 CN 当成客户身份。由于
  外层 TLS listener 在请求进入内层明文 gRPC server 前已终止 TLS，现通过只接受已验证
  `tls.ConnectionState` 的连接生命周期 registry/stats handler 把证书身份绑定到 RPC
  context；连接关闭时按注册 generation 清理，避免地址复用导致旧连接误删新身份。
- **TLS A25 三副本安全转发与证书鉴权故障恢复（2026-07-16）**：TLS-only 部署不再
  隐式降级到明文，只有显式 mixed mode 才允许 fallback；follower 按 leader client
  port 建连，并用 gRPC authority 保留稳定 service DNS 的证书校验名。针对新版
  `grpc.NewClient` 的 lazy dial，连接探测改为带 5 秒边界的轻量 Maintenance Status：
  transport 错误判失败，auth 等应用层拒绝证明 mTLS/HTTP2 已就绪。真实 kind + 独立
  TiKV/PD 三副本验证全部 Ready，逐个删除三只原 Pod 后每轮 TLS Put/Get 均恢复；启用
  auth 后，用独立 CN `cert-root` 证书经 service 及逐 Pod 直连 Put/Get 全部通过，覆盖
  leader 和 follower 转发路径。TLS handshake 也设置 10 秒上限，避免半开连接长期阻塞
  外层 listener。
- **TLS A26 服务端与内部客户端叶证书热轮换（2026-07-16）**：对齐 etcd transport
  的 per-handshake reload：启动时仍预检证书/私钥并 fail fast，但 runtime
  `tls.Config.Certificates` 保持为空，通过 `GetCertificate` 在每个新入站 handshake
  重读服务端 key pair，通过 `GetClientCertificate` 在 follower/peer 每个新出站 mTLS
  handshake 重读客户端 key pair。这样 Kubernetes Secret/projected volume 更新后无需
  重启 Pod；即使客户端不发送 SNI（例如只按 IP 校验证书）也会触发 reload。测试以同一
  CA 连续替换不同 serial/CN 的证书，证明入站和出站连接均看到新叶证书；损坏的替换
  文件在下一次 handshake 明确失败，不会静默继续使用旧身份。
- **TLS A27 CA trust pool 双信任窗口与撤旧（2026-07-16）**：为 DBaaS Secret
  轮换补齐无需重启的 CA rollover。入站 listener 在每个 ClientHello 重读 `ClientCAs`
  并克隆不可变基础配置；出站 follower/peer 因 Go TLS 没有 RootCAs callback，改在每个
  handshake 的 `VerifyConnection` 重读 roots，并显式执行完整 x509 chain、intermediate
  和 DNS name 校验。测试覆盖 `旧 CA -> 旧+新 bundle -> 新 CA`：overlap 阶段新旧
  client/server chain 均可连接，撤旧后旧 chain 被拒绝、新 chain 保持可用；错误 hostname
  和损坏 CA 文件也必须拒绝，不能回退缓存 trust。已建立的 TLS/HTTP2 长连接不会被主动
  中断，撤旧在下一次 reconnect 生效，控制面需先发布 overlap bundle、轮换 leaf，再撤旧。
- **TLS A28 真实三副本在线轮换与拒绝隔离（2026-07-16）**：kind + 独立 TiKV/PD
  黑盒把 client/peer Secret 从旧 CA 依次推进到 `旧+新 CA`、新 leaf、仅新 CA，并逐 Pod
  校验 projected 文件 hash 后发起新连接。overlap 阶段新 CA client 可访问仍使用旧 leaf
  的三副本；leaf 更新不触发 rollout，service 和三个 Pod 直连均 Put/Get 成功；撤旧后新
  chain 可用、旧 client 在服务端留下明确证书拒绝，随后同一进程立即接受新 chain。测试
  还逐个替换轮换前的三只 Pod，每轮 rollout 后 follower/leader 转发均恢复，最后开启 auth
  并用新 CA 签发的 `cert-root` 经 service 和三个 Pod 直连成功。该负向测试同时发现外层
  identity TLS listener 曾把单连接 handshake 错误返回给 cmux、导致整个 secure endpoint
  退出；现 handshake/deadline 错误只关闭该连接并继续 Accept，单测固定“先拒绝 malformed
  client、再接受正常 client”，避免不可信客户端造成监听器级 DoS。
- **TLS A29 长连接退场边界与轮换 soak（2026-07-16）**：etcd 的 keepalive interval/
  timeout 只检测死连接，健康 watch/lease HTTP2 transport 可无限保留旧 TLS trust。新增
  opt-in `--grpc-max-connection-age` / `--grpc-max-connection-age-grace`，默认 age=0 保持兼容；
  启用时 grace 必须为正，client/peer server 到龄先 GOAWAY、grace 后关闭旧 streams。
  production TLS manifest 使用 `1h/5m`，把 CA 撤旧生效上界限制为 65 分钟。in-process
  官方 client/v3 测试用连接 stats 证明 transport generation 增加，并验证原 watch 和
  lease keepalive 在强制退场后继续。真实三副本把该值加速为 `5s/2s`，一个 client/v3
  3.5.2 长连接跨叶证书和 CA rollover、旧证书拒绝、三 Pod 逐个替换持续写入 229 次且
  lease 未丢、watch 从最后 revision 恢复，随后新 CA cert auth 全端点通过。需注意旧版
  grpc 在 max-age 强关时可能把 GOAWAY+EOF 暴露为 `Unknown`，consumer 必须按最后 revision
  重建 watch、按同一 lease ID 重建 keepalive；新版 client 可透明恢复。控制面应根据最长
  请求/stream 选择 grace，并监控重连/全量 relist 峰值，不能把生产 age 调到 smoke 量级。
- **TLS A30 TLS policy 与动态 CRL 吊销（2026-07-16）**：对齐 etcd 全局
  `--tls-min-version`（默认 TLS1.2）、`--tls-max-version`、`--cipher-suites`，统一应用于
  client/peer/info 的 server 和内部 client；仅接受 TLS1.2/TLS1.3，拒绝 min>max、未知
  cipher，以及 TLS1.3-only 下配置 Go 不允许定制的 cipher suites。新增 etcd 同名
  `--client-crl-file`、`--peer-crl-file` 及 info 扩展 `--info-crl-file`：启动预检 DER CRL，
  每次 handshake 重读，且在正常 chain/hostname 验证后检查所有 presented certificates
  的 serial；损坏文件 fail closed。单测覆盖 TLS1.2/TLS1.3 negotiation、cipher 匹配/不
  匹配、入站 client 与出站 server 吊销、CRL 动态替换和 malformed CRL。真实 kind 三副本
  在 CA/leaf rollover、5s max-age、三 Pod 替换及 201 次 lease+watch 持续写后开启 auth，
  动态投影新 CRL 吊销一个 `cert-root` 证书：该 serial 被拒绝，而同 CN/同 RBAC 的另一
  张未吊销证书立即 Put/Get 成功，证明结果来自证书级吊销而非用户权限变化。
- **TLS A31 独立出站身份与 peer 身份白名单（2026-07-16）**：新增 etcd 同名
  `--client-cert-file`/`--client-key-file` 与 `--peer-client-cert-file`/
  `--peer-client-key-file`，未配置时仍回退到各 listener 的 serving pair；两套路径均在每次
  handshake 重读，支持 Secret 原地轮换。新增 `--client-cert-allowed-hostname`、
  `--peer-cert-allowed-cn`、`--peer-cert-allowed-hostname`，CN 与 hostname 模式互斥，且只在
  显式 trusted CA 完成 chain 验证后匹配 leaf；无 client-auth/CA 的无效白名单启动即拒绝。
  单测覆盖 split identity、动态替换、CN/SAN 接受与拒绝、同身份但非可信 CA 拒绝。真实三
  副本 smoke 使用 `kubebrain-outbound` 独立证书完成 follower 转发，并在旧+新 CA overlap、
  serving/outbound leaf 更新、撤旧和 Pod 替换全过程由 peer CN 白名单持续约束。
- **限额 A32 请求字节与 txn 操作数（2026-07-16）**：新增 etcd 同名
  `--max-request-bytes`（默认 `1572864`）与 `--max-txn-ops`（默认 `128`）。client 与
  peer listener 都设置 payload+512 字节的 gRPC 接收边界，unary 在进入 auth/storage 前按
  protobuf payload 精确判断，超限返回 etcd 原文 `InvalidArgument: etcdserver: request is too
  large`；follower 转发使用同一发送上限，避免自定义大限额只在 leader 可用。txn 的 compare/
  success/failure 与嵌套预算使用实例配置，不再硬编码 128。单测覆盖 limit 边界、handler 未
  调用、真实 gRPC wire error、嵌套 txn、flag 绑定与整数溢出拒绝。该限制保护单请求内存和
  TiKV transaction 放大，不等同于容量/QPS/watch 配额。真实 kind 三副本用官方
  client/v3 发送“value 等于上限、加 protobuf overhead 后超限”的 Put，得到精确
  `ErrRequestTooLarge`；随后 CA/serving+outbound leaf 轮换、六轮 Pod 替换、200 次 lease+
  watch 持续写、auth 与动态 CRL 全部通过，证明限制不会破坏 follower/HA 路径。
- **传输 A33 HTTP/2 stream 与 keepalive 策略（2026-07-16）**：新增 etcd 同名
  `--max-concurrent-streams`（默认 `math.MaxUint32`）、`--grpc-keepalive-min-time`（`5s`）、
  `--grpc-keepalive-interval`（`2h`）、`--grpc-keepalive-timeout`（`20s`），统一应用于 client/
  peer listener。min-time 仅正数时启用；interval 与 timeout 必须都为正才发 server ping，任一
  为 0 即关闭，保持 upstream 条件语义；已有 max-connection-age 与 ping 参数合并到同一
  `keepalive.ServerParameters`，避免后注册 option 覆盖前者。真实 bufconn HTTP/2 测试以
  max=1 持有 health watch，证明同连接 unary 被阻塞至 deadline，取消 watch 释放 slot 后
  立即成功；flag/default 测试固定 upstream 数值。真实 kind 三副本以 production manifest
  的显式 keepalive/stream flags 运行，跨 CA/leaf 轮换、六轮 Pod 替换与 max-age GOAWAY 完成
  193 次 lease+watch 持续写，随后 auth/CRL 流程通过。
- **Auth A34 token TTL 与密码 hash 策略（2026-07-16）**：新增 etcd 同名
  `--auth-token`（默认/当前仅 `simple`）、`--auth-token-ttl`（默认 `300s`）与
  `--bcrypt-cost`（默认 `10`）。TTL 进入持久化共享签名 token 的 `exp`，所有副本用同一
  TiKV signing key 验证；0 或 duration 非正溢出回退 300s。新增/修改明文密码按配置 cost
  bcrypt，`hashed_password` 保持调用方提供的 hash；cost 超出 bcrypt `[4,31]` 时与 etcd
  一样回退 10。KubeBrain 的 `simple` 是为无共享内存的多副本数据面实现的签名 bearer
  token，保证 API 生命周期语义但不承诺 token 字节格式等同单进程 etcd opaque token。
  单测覆盖 hash cost、无效 cost fallback、TTL 边界与 flag/default/配置传递；真实三副本
  auth smoke 将 TTL 加速到 2s，固定“新 token 成功→过期返回 InvalidAuthToken→重新认证成功”。
- **Auth A35 JWT token provider（2026-07-16）**：`--auth-token=jwt,...` 兼容 etcd 的
  `sign-method`、`pub-key`、`priv-key` 与 `ttl` 配置，支持 HMAC、RSA、RSA-PSS、ECDSA 和
  Ed25519；非对称算法允许仅公钥的 verify-only 副本，并在启动时校验算法、key material
  及公私钥匹配。JWT 携带 username、expiry 与 auth revision，任何 RBAC mutation 后旧 token
  与 upstream 一样返回 `etcdserver: revision of auth store is old`；证书 CN 转发也按所选 provider
  签发。算法/expiry/key mismatch、配置 fail-loud 和 auth revision 均有单测与 race 覆盖；同一
  HS256 配置对 reference etcd 与 KubeBrain 连跑 10 次差分通过。真实 kind 三副本还固定了
  单副本签发 token 可在全部副本验证，并在删除当前 Leader 后继续读写。生产多副本必须将
  同一签名 key 以 Secret/KMS 管理的只读文件挂载到所有副本。
- **KV A36 standalone Put 差分与 leased PrevKV（2026-07-16）**：对照
  `/root/etcd/server/etcdserver/api/v3rpc/key.go:checkPutRequest` 与
  `server/etcdserver/txn/put.go`，新增官方 client/v3 双端 `TestPutDifferentialAgainstReferenceEtcd`。
  覆盖 create/update revision、`PrevKv`、lease 绑定/换绑、`IgnoreValue`、`IgnoreLease`、
  missing lease/key 以及空 key/冲突 option 的 code/message。差分发现原子 leased Put 虽将
  value 与 attachment 一批提交，却丢弃 `PrevKv`；现直接复用同一 `TxnApply` 的 pre-read
  结果构造响应，不增加第二次非原子读取。内存后端回归固定完整旧 value/lease/revision，
  kind 三副本独立 TiKV/PD 与 reference etcd 提交 `d947b2086` 连续 10 轮结构化差分通过。
- **KV A37 linearizable historical Range header（2026-07-16）**：全量双端差分发现，
  revision=4 的历史 KV 在当前 revision=5 时，etcd 返回 `Header.Revision=5`，而请求被 LB
  分配到落后 follower 时 KubeBrain 曾返回其 durable watermark 4。根因是 bounded
  historical follower 快路径未限制 `Serializable`，误把“历史内容已持久化”当成“线性读
  header 已追平”。现仅 `Serializable=true` 可直接读 follower durable snapshot；默认
  linearizable historical Range 在可代理部署中转发 Leader，否则先完成 read-index sync。
  单测分别固定 follower 快路径与 Leader proxy header；真实三副本 DeleteRange+历史 Range
  双端差分连续 20 轮通过，随后 Compact/Delete/Lease/Put/Range/RangeStream/serializable
  Range/Txn/Watch control 全矩阵同轮通过。
- **Lease A38 standalone DeleteRange attachment 原子性（2026-07-17）**：审计发现普通
  DeleteRange 先提交用户 tombstone，再 best-effort 删除 `leasekeys/<key>`；两者之间崩溃会让
  新副本从 TiKV 重载已删除 key 的陈旧 attachment。现 DeleteRange 在绑定快照与提交期间独占
  lease write fence；范围含 leased key 时按 128 user keys 分块，以同一 `TxnApply` 将用户
  tombstone 和 attachment internal delete 原子提交，精确 mod-revision guard 防止并发覆盖，
  `PrevKv` 直接取同批 pre-read。故障注入拒绝 batch 时证明 value/attachment 同时保留，重试后
  deletion count、旧 value/lease 和 `LeaseTimeToLive(Keys=true)` 与 etcd 一致；扩展 lease 双端
  差分连续 10 轮、完整差分矩阵通过。真实三副本删除后全量滚动重启，存活 lease 从 TiKV
  恢复后 keys 仍为空。generic Txn 的旧 sequential fallback 当时尚未纳入本保证，后续由 A39 收敛。
- **Lease A39 Txn fast-shape attachment 原子性（2026-07-17）**：继续审计 A38 留项发现，
  Kubernetes 风格的 create/update/compare-delete fast shape 会绕过 generic `TxnApply`，先提交
  用户值再调用 `bindKeyToLease`/`unbindKeyFromLease`，仍存在相同崩溃窗口。dispatcher 现检测
  请求 lease 或 key 当前 binding：任何会新增、换绑、清除 attachment 的 fast shape 均回落到
  guarded atomic generic path；纯 leaseless fast shape 保留原优化。generic 单写绕过也已删除，
  因而所有含写 generic Txn 只走 atomic/staged executor，cursor fallback 仅处理无写分支；重叠
  Put/Delete 继续按 etcd 在验证层返回 `InvalidArgument: duplicate key given in txn request`。
  故障注入分别拒绝 leased create/delete batch，证明 key 与 attachment 不会单边出现或消失；
  新增双端场景覆盖 create、compare-delete+PrevKV、TTL attached keys 与 duplicate rejection，真实
  TiKV/PD 三副本对 reference etcd 连续 10 轮一致。
- **Compact A40 revision 0/负数边界（2026-07-17）**：对照
  `/root/etcd/server/storage/mvcc/kvstore.go:updateCompactRev` 发现，etcd 将首次
  `Compact(0)` 作为 revision 0 的合法逻辑压缩，后续重复请求返回 Compacted；KubeBrain
  backend 却把 0 当作“当前 revision”哨兵，负数经 `uint64` 转换后也被 clamp 到当前值，
  两者都可能意外删除全部历史。现 backend 将 0 作为字面 revision，并以 compact key 是否
  存在区分“尚未压缩”和“已压缩到 0”；RPC 在任何有 marker 的 `request <= watermark` 返回
  `OutOfRange: required revision has been compacted`，负数在转换前直接拒绝。回归测试证明首次
  zero compact 不删除已有历史、marker 已持久化、重复 zero 精确报错，negative 不写 marker
  且不影响历史；Compact 双端差分增加负数 code/message 对比。
- **Maintenance A41 HashKV signed revision/metadata（2026-07-17）**：继续 signed
  revision 审计，对照 `/root/etcd/server/storage/mvcc/kvstore.go:hashByRev` 与真实 server
  确认 `HashKV(-1)` 不是 current alias，而是对空 MVCC revision 前缀求 hash；响应仍以当前
  revision 填 Header，并显式返回 `HashRevision=-1`。KubeBrain 旧接口使用 `uint64`，把所有
  非正请求都改写为 current，且从未填 `HashRevision`；未做过 compaction 时还错误返回
  `CompactRevision=0` 而非 etcd 的 `-1`。backend hash API 现保留 signed revision：0 选择
  线性化当前快照，负数不纳入任何用户 version；server 返回实际 hashed revision，并用 durable
  compact marker 区分 `-1`、0 与正 watermark。单元测试固定 empty-prefix Castagnoli CRC、
  current/historical/negative metadata 和未压缩状态；新增官方 client/v3 双端差分，不比较不同
  物理编码的 current hash 数值，只比较 negative hash、signed revision、header gap 与选择关系。
- **Rollout A42 serving readiness（2026-07-17）**：真实三副本滚动发布中复现 NodePort
  `Unavailable: proxy is not ready`。Leader 取得 election ownership 后，`/ready` 曾在 durable
  state reload、event-log 初始化和 physical-compaction resume 完成前直接返回 200；Follower 的
  proxy readiness 也只核对 leader identity，在旧 Leader 退出后仍可能保留已断开的 gRPC transport。
  现 Leader readiness 同时要求本地 gRPC health 为 `SERVING`，该状态只在 durable startup 全部
  成功后发布，并在失去 leadership 时撤销；Follower readiness 要求当前 Leader 对应的 active
  connection 为 `READY`，`IDLE` channel 会主动发起连接，其余状态均先从 Service endpoints
  撤流。HTTP 状态转换和真实 gRPC server 断连均有确定性回归，kind + 独立 TiKV/PD 继续以
  in-cluster 并发 client/v3 load 覆盖整轮三副本 rollout。
- **Maintenance A43 Status default quota（2026-07-17）**：官方 `etcdctl endpoint status`
  发现 KubeBrain 广告 3.7 协议却遗漏 3.6 起的 `DbSizeQuota`，输出 quota 为 0；同配置下
  reference etcd 根据 `/root/etcd/server/etcdserver/api/v3rpc/maintenance.go:Status`
  回落到 `storage.DefaultQuotaBytes=2 GiB`。现 Status 返回相同默认 quota sentinel，并继续以
  `DbSize=DbSizeInUse=1` 表示无 bbolt fragmentation；该值只维持协议与工具兼容，不伪装
  TiKV 容量或实例售卖限额，真实 store disk/region/quota 仍由 PD/TiKV 指标和 DBaaS 控制面
  提供。server 回归固定非零默认值，官方 client/v3 双端差分在修复前稳定得到
  `reference=2147483648, KubeBrain=0`，修复后要求两端精确相等。
- **Maintenance A44 Status leadership term（2026-07-17）**：A43 的真实 `etcdctl`
  输出继续暴露 `RaftTerm=0`，而 reference etcd 从首次 election 起始终返回正 term。
  KubeBrain 现读取 client-go resource lock 中由所有副本共享、每次 holder identity 变化递增的
  `LeaderTransitions`，并以 `LeaderTransitions+1` 填 `Status.RaftTerm`；因此首次任期为 1，
  failover 后严格递增，Leader 与 Follower 读取同一值。该 term 仅是 etcd 客户端可观察的
  leadership generation；写 fence 继续使用每进程原子递增且带 renew freshness 的 local epoch，
  两者不混用。确定性测试覆盖初始/多次切换、负 record 和存储失败 fail-closed；官方 client/v3
  双端检查两端 term 均为正；真实三副本逐 Pod Status 在删除 Leader 前全部为 63，替换并完成
  failover 后新旧三副本全部收敛到 64，同时 leader identity 与 revision 一致更新。
- **Watch A45 RequestProgress stream ID（2026-07-17）**：raw gRPC 双端差分在创建单个
  `watch_id=51`、接收该 key 的 Put event 后请求 progress，reference etcd 根据
  `/root/etcd/server/storage/mvcc/watchable_store.go:progressAll/progressIfSync` 返回一个
  stream-wide `WatchId=-1` response，KubeBrain 曾直接返回 `WatchId=51`；无 active watch
  时还会伪造 `-1` response，而 etcd 不发送。现 RequestProgress 先 kick FIFO progress marker，
  单 watch stream 立即以该 watch 的 delivered watermark 发送标准 `-1`；multiplexed stream
  最多等待 100ms，全部 watches 都到达 captured published revision 后以最慢 watermark 发送
  `-1`，空 stream 保持静默。若大规模 multiplexed stream 中仍有 lagging watch，则保留逐
  watch 安全 watermark 的扩展响应，避免慢 initial sync 阻塞其他
  kube-apiserver cacher；任何 stream-wide revision 都不会越过最慢 watch。确定性测试覆盖
  synchronized floor、laggard timeout 与 empty stream，完整 server race 通过；exact-image
  三副本 TiKV/PD 上 raw control 双端差分连续 20 轮一致，普通 history/update watch 同轮通过。
- **Watch A46 Follower control revision fence（2026-07-17）**：raw gRPC 双端差分覆盖
  created、duplicate ID、negative revision、第二个 created 与显式 cancel，发现请求落到
  Follower 时 KubeBrain 的 control response Header revision 全为 0，而 reference etcd 的
  `/root/etcd/server/etcdserver/api/v3rpc/watch.go:newResponseHeader` 始终使用 stream 当前
  revision。Follower 的 from-now proxy watch 同时从 0 启动，在读取 Leader revision 与完成
  注册之间存在漏过新事件的窗口。现每次 create/cancel 先向 Leader 执行 read fence 得到 R，
  stream control response 至少报告 R，from-now 请求改为从 R+1 注册；Leader 侧只使用已发布
  watermark，避免 Header 超前于 watch event log。compacted/internal cancel 继续保留 etcd 的
  zero Header，client cancel 使用最新 control fence。确定性测试固定 R+1 订阅和 cancel Header，
  raw 双端差分同时比较 Header 是否为 zero，并在 exact-image 三副本 TiKV/PD 上验证。
- **Maintenance A47 ResponseHeader leadership term（2026-07-17）**：逐字段比较
  `etcdctl endpoint status -w json` 发现 KubeBrain 的顶层 `Status.RaftTerm` 已为正，但
  `Status.Header.RaftTerm=0`；reference etcd 在
  `/root/etcd/server/etcdserver/api/v3rpc/header.go:fillWithoutRevision`、
  `watch.go:newResponseHeader` 和 `v3_server.go:newHeader` 中为所有 unary/stream Header
  填当前 term。现 leader-election 每次观察 TiKV-backed resource-lock record 时原子缓存
  `LeaderTransitions+1`，client gRPC unary/stream interceptor 与 ClusterId/MemberId 一并统一
  填充，避免每个 RPC 额外执行 election storage read 和 TSO。缓存尚未初始化时同步读取并
  fail-closed，不发送 term 0；确定性测试覆盖 lock observation、unary/watch Header 和无 race。
  exact-image 三副本在 failover 前均报告 Header/top-level term 72，删除 Leader 后新旧副本
  全部收敛到 73；官方 client/v3 双端差分连续 20 轮通过，并检查普通 Range/Watch Header term 为正。
- **Cluster A48 MemberList Header revision（2026-07-17）**：对照
  `/root/etcd/server/etcdserver/api/v3rpc/member.go:ClusterServer.header` 确认 Cluster RPC
  Header 只含 ClusterId、MemberId 与 RaftTerm，不携带 MVCC revision。KubeBrain 曾把
  TiKV TSO-backed current revision 填入 MemberList，官方 client/v3 双端差分在先写入正
  revision 后稳定复现 `reference=0`、KubeBrain 为大正数；现移除该字段，保留 interceptor
  统一填 term。确定性/race/full/vet 均通过，exact-image 三副本逐 Pod 均返回 revision=0、
  term=74，双端差分连续 20 轮通过；in-cluster `clientv3.Sync` 后 Put/Get 连续 20 轮通过。
  本次开发 Deployment 未配置 `--initial-cluster`，动态 fallback 在 Leader 仅列自身、Follower
  仅列自身与 Leader，不能证明三成员 AutoSync；生产 DBaaS 控制面仍必须为稳定身份的所有副本
  下发相同完整 membership，或后续实现等价的全副本服务发现，不能把该 fallback 当成完成态。
- **Maintenance A49 Defragment empty Header（2026-07-17）**：raw/official client
  检查发现 KubeBrain 为 Defragment 合成 current revision Header，而 reference etcd 的
  `/root/etcd/server/etcdserver/api/v3rpc/maintenance.go:Defragment` 明确返回空
  `DefragmentResponse{}`，Header 为 nil；`etcdctl defrag` 不显示 response body，普通 CLI
  smoke 会漏掉该 wire-shape 差异。现 KubeBrain 同样返回空 response，中央 Header interceptor
  明确保留 nil，不为刻意省略 Header 的 RPC 伪造 ClusterId/MemberId/RaftTerm。server 单测固定
  nil shape，完整/race/vet 通过；官方 client/v3 双端 maintenance 差分连续 20 轮一致，
  exact-image 三副本逐 Pod 均验证 Header=nil，其他 Status/Range/Watch Header term 检查仍通过。
- **Lease A50 KeepAlive response revision fence（2026-07-17）**：对照
  `/root/etcd/server/etcdserver/api/v3rpc/lease.go:LeaseKeepAlive` 的明确注释和实现，
  KeepAlive 必须在发起 renew 前捕获 Header revision；否则 renew 成功后并发 Revoke 已在
  revision R 可见，迟到的 Header 仍可能报告 R 或更高，使客户端误判该 lease 在该 revision
  仍存活。KubeBrain 原先先 `refreshLease`、后读 current revision；现每个 stream message 在
  Recv 后、鉴权和 renew 前固定 response revision，Follower proxy 继续保留 Leader 返回的
  Header。确定性测试以 `leaseMu` 阻塞 renew，确认 revision read 已先发生，再人为推进 backend
  revision 并释放锁，最终 response 仍保持旧值；该测试连续 race 通过。完整 server/vet 通过，
  exact-image 三副本 TiKV/PD 上官方 client/v3 Lease 全生命周期双端差分连续 20 轮一致。
- **Cluster A51 production stable membership（2026-07-17）**：A48 记录的控制面缺口在
  `deploy/production/kubebrain*.yaml` 中真实存在：三副本仍是无稳定身份的 Deployment，且未
  下发 `--initial-cluster`，所以生产样例无法满足自身的 MemberList/AutoSync 契约。plain/TLS
  两套现统一改为三副本 StatefulSet，以 `kubebrain-peer` headless Service（
  `publishNotReadyAddresses=true`）提供稳定 Pod DNS；downward API 注入 `POD_NAME`，
  `--advertise-host` 使用 ordinal FQDN，所有副本下发完全相同的三成员 initial-cluster（TLS
  版本使用 https peer URL）。结构化 YAML 回归解析对象并固定 workload kind、replicas、
  identity/member args、fieldRef，以及 client 普通 ClusterIP/peer headless 的职责分离；
  两份 manifest 的 kubectl client dry-run 均通过。隔离 keyspace 的真实 kind 部署中，逐 Pod
  MemberList 返回相同三个 ID/URL，in-cluster clientv3 Sync+Put/Get 连续 20 轮通过；删除
  Leader ordinal 并等待新 UID 与 EndpointSlice ready 后，重建 Pod 保持原 member ID，完整
  membership 与 Sync 再连续 20 轮通过。仅等待旧 Pod 的 Ready condition 会误判替换完成，
  failover 自动化必须同时核对 UID/endpoint generation。
- **Lease A52 list expiry order（2026-07-17）**：对照
  `/root/etcd/server/lease/lessor.go` 的 `Leases`（按 `leasesByExpiry` 排序）及
  `/root/etcd/tests/integration/v3_lease_test.go` 的 `TestV3LeaseLeases`，发现
  KubeBrain `LeaseLeases` 直接遍历 Go map，官方 client 观察到的 lease 顺序随机。新增
  `TestLeaseLeasesOrdersByExpiryLikeEtcd`，人为设置与 ID/map 顺序不同的 deadline，修复前
  连续 20 轮稳定失败；现持 `leaseMu` 快照 active lease 后按绝对 deadline 排序，相同
  deadline 以 ID 作确定性 tie-break，不改变过期调度或 TTL。官方 client 双端测试以
  303/301/302 秒乱序 Grant 验证相对位置：旧 exact image 连续 10 轮中失败 9 轮，修复后的
  三副本 TiKV/PD exact image 连同既有 Lease 全生命周期差分连续 20 轮全部通过；focused
  race 与完整 server/full/vet 通过。
- **Lease A53 auth revision fence（2026-07-17）**：对照
  `/root/etcd/server/etcdserver/v3_server.go` 的 `checkLeaseRenew`、
  `checkLeaseTimeToLive` 和 `checkLeaseLeases`，发现 KubeBrain 虽会以 request-local auth
  snapshot 检查每个 attached key，但 TTL(Keys=true)/List 返回及 KeepAlive renew 前未校验
  auth store revision；并发撤权时旧 snapshot 因而还能泄露一次 key 名或延长一次 lease。
  三条路径现共用 `authorizeLeaseKeys`：先按 etcd 分别检查 READ/WRITE，再调用
  `ensureAuthRevision`，revision 已变化则返回 `ErrAuthOldRevision`，由客户端重试并按新权限
  判定；与 etcd 相同，未启用 auth 及 root/admin caller 直接放行。确定性测试先取得仍允许
  READWRITE 的普通 caller，再撤销 permission，证明 snapshot 自身仍放行但 TTL/List 与
  renew 共用的 revision fence 拒绝 stale caller，同时 stale root caller 仍通过；focused
  单元连续 20 轮和 race 通过。独立 keyspace 的 TiKV/PD exact image 上，官方 client Auth
  lifecycle 新增 attached-key TTL、LeaseLeases 与 KeepAliveOnce 后完整通过，确认合法鉴权
  流量不受影响；临时 auth 实例已销毁。
- **Lease A54 grant publish-after-commit（2026-07-17）**：对照 etcd
  `/root/etcd/server/etcdserver/v3_server.go` 的 `LeaseGrant -> raftRequest` 及
  `apply/backend.go` 的 `Lessor.Grant` apply 顺序，发现 KubeBrain 原先先把 lease 放入 active
  map 并启动 timer，再提交 TiKV meta。显式 ID 的并发 Put 因而可在 Grant 尚未 durable 时
  成功，若 meta 随后失败，该已确认 Put 又会被 grant cleanup 删除。现以 `pendingLeases`
  单独预留 ID：并发 duplicate Grant 仍返回 `lease already exists`，但 Put/TTL/List 均只看
  committed active map；InternalPut 成功后才设置 deadline、发布并启动 timer。失败路径用保留
  原 leadership epoch 的 fresh bounded context 尝试清理 commit-undetermined meta，再释放
  reservation；lease-state generation 阻止跨 `StopLeases`/reload 的迟到 commit 重新污染
  follower snapshot。阻塞 meta 测试证明 commit 前 Put 返回 `lease not found`、commit 后可用；
  cancel 测试证明失败不泄漏 active/pending 且同 ID 可重试；state reset 测试证明迟到成功返回
  `Unavailable` 且不发布。三组 focused 连续 50 轮及 race 连续 10 轮通过，三副本 TiKV/PD
  exact image 上官方 Lease lifecycle/order 双端差分连续 20 轮通过。
- **Lease A55 revoke/regrant ID reuse fence（2026-07-17）**：etcd 的 LeaseGrant/Revoke
  都经同一 Raft apply 顺序执行；KubeBrain revoke 虽持 `leaseWriteMu` 删除 keys 与 meta，却会
  在 TiKV `InternalDelete(meta)` 完成前先从 active map 移除 ID，而 Grant 原先不参与该锁。
  同 ID regrant 因而可在旧 delete 阻塞期间写入新 meta，随后被旧 revoke 删除；内存中 replacement
  暂时可用，leader reload 后却消失。LeaseGrant 现从 ID reservation、meta commit 到 active
  publish 全程持 `leaseWriteMu.RLock`，与 revoke/expiry 的 write lock 排序，同时不同 lease 的
  grant 仍可并发。确定性测试阻塞旧 meta delete，确认 same-ID grant 必须等待，随后 revoke 与
  regrant 均成功，并以 `ReloadLeases` 证明 replacement 的 60 秒 meta 持久存在；focused 连续
  30 轮及覆盖 grant/revoke/expiry/leased-Put 的 race 连续 10 轮通过。官方 client 双端 Lease
  差分新增显式 ID Grant -> Revoke -> 同 ID Grant(301s) -> TTL，并在三副本 TiKV/PD exact
  image 上连同完整 lifecycle/order 连续 20 轮通过。
- **Lease A56 revoke apply-time authorization（2026-07-17）**：对照
  `/root/etcd/server/etcdserver/apply/auth.go` 的 `authApplierV3.LeaseRevoke`：
  `checkLeasePuts` 在同一 apply 顺序中检查 lease 当时附着的全部 key。KubeBrain 原先在取得
  `leaseWriteMu` 前读取 keys 并鉴权；另一个已获 RLock 的 Put 可在检查后提交新的 protected
  key，随后有限权限调用者的 revoke 会越权删除它。现 leader 路径先取得 exclusive
  `leaseWriteMu`，等待所有 admitted binding write 完成，再对最终 key set 检查 WRITE 权限并
  在同一锁区间调用 `revokeLeaseLocked`；auth config 的 backend guard 继续覆盖检查后的并发
  RBAC 变更。确定性测试阻塞 root 对 `/denied/` leased key 的 TiKV commit，同时让仅有
  `/allowed/` 权限的 Alice revoke：revoke 必须等待 Put，之后返回 `PermissionDenied` 且 key
  保留；focused 连续 30 轮与覆盖 auth/revoke/regrant/leased-Put 的 race 连续 10 轮通过。
  独立 keyspace 的 exact image 上官方 client Auth lifecycle 新增同一 protected-key 场景并
  通过，root 随后可正常 revoke 清理；临时 auth 实例已销毁。
- **Watch A57 automatic ID allocator（2026-07-17）**：对照
  `/root/etcd/server/storage/mvcc/watcher.go` 的 `watchStream.Watch`，发现 KubeBrain
  每次从当前 active map 中选择最小空闲 ID，cancel 后会复用旧 automatic watch ID；
  etcd 则维护 stream-scoped `nextID`，仅跳过仍被显式 ID 占用的位置，不回收已 cancel
  的 automatic ID。现每个 multiplexed stream 持有单调 `nextWatchID`，automatic create
  从 0 开始递增并跳过 active explicit ID。确定性单元测试覆盖 `0 -> cancel -> 1` 及
  explicit `2` 仍 active 时下一 ID 跳至 `3`；raw gRPC 双端差分加入 automatic create、
  cancel、再次 create。该差分同时暴露 client cancel 删除 watch 后 backend goroutine 仍会
  发送第二个 `watch closed` cancel；现以 map removal 作为唯一完成点，所有已移除 ID 的迟到
  internal cancel 与未知 client cancel 均静默，确保每个 watch 最多一个 cancel response，
  并校验 reference etcd 与 KubeBrain 的 control response 序列一致。focused 连续 50 轮、
  control-path race 连续 10 轮及 full/vet 通过；三副本 TiKV/PD exact image
  `d0d54fdcc7c6` 上 raw gRPC 双端差分连续 20 轮通过。
- **Watch A58 configured fragmentation ceiling（2026-07-17）**：对照
  `/root/etcd/server/etcdserver/api/v3rpc/watch.go` 的 `sendFragments` 与
  `MaxRequestBytesWithOverhead`，发现 KubeBrain 对 `Fragment=true` 的 watch response
  固定使用 2 MiB 分片阈值，虽然碰巧等于 etcd 默认的 `1572864+512KiB`，却不会随 DBaaS 实例的
  `--max-request-bytes` 配额变化。审计同时发现 KubeBrain 将 etcd 的 512 KiB gRPC
  transport allowance 误写成 512 bytes，影响 server receive 与 follower proxy send 上限；
  现统一修正为 `512*1024` 并同步 overflow validation/CLI 文案。Watch 复用该 admission 常量，
  每个 stream 按 server 配置计算 response fragment ceiling。新增单元测试锁定配置传播，
  raw gRPC 双端差分则分别写入两个 800 KiB value，再以小型 prefix DeleteRange 请求 `PrevKv`：
  放大的 watch response 必须按 reference etcd 相同的 event 数与 `Fragment` flag 分片。
  另以超过旧 512-byte transport window、但位于 etcd 512-KiB allowance 内的请求验证稳定返回
  `RequestTooLarge` 而非 transport `ResourceExhausted`。focused 连续 50 轮、相关 race 连续
  10 轮及 full/vet 通过；1 MiB 自定义上限下，旧 A57 稳定复现 `[2]/[false]` 对 reference
  `[1,1]/[true,false]`，三副本 TiKV/PD exact image `122c002bf172` 双端差分连续 20 轮通过。
- **Watch A59 periodic progress elision（2026-07-17）**：对照
  `/root/etcd/server/etcdserver/api/v3rpc/watch.go` 的 `sws.progress` 状态机：启用
  `ProgressNotify` 的 watch 在发送 event 后将 eligibility 置 false，下一次 ticker 仅重新
  arm 而不发送冗余 progress，再下一 tick 才对持续 quiet 的 watch 通知。KubeBrain 原先每个
  tick 无条件发送。现每个 watch goroutine 维护同构状态，成功交付非空 event batch 后 suppress
  exactly one tick；新增纯状态机单元测试覆盖 quiet/event/multi-event 序列，并以官方
  client/v3 从首个 progress 对齐 tick，相隔 200ms 写入 watched key，验证 event 后 1200ms
  无 progress、随后 1500ms 内恢复通知。旧 A58 exact image 在首个 post-event tick 稳定
  复现冗余 progress；状态机单元连续 100 轮、相关 Watch pipeline race 连续 10 轮及
  full/vet 通过，三副本 TiKV/PD exact image `dacfe140d352` 官方 client 场景连续 10 轮通过。
- **Watch A60 minimum progress interval（2026-07-17）**：对照
  `/root/etcd/server/etcdserver/api/v3rpc/watch.go` 的 `minWatchProgressInterval=100ms`，
  etcd 对正数但低于下限的 `--watch-progress-notify-interval` 发 warning 并 clamp；KubeBrain
  原先会按 `1ms` 等输入直接同时驱动 backend marker 与 per-watch ticker，形成可配置的 CPU/
  fan-out storm。现 backend config completion 保留 `<=0 -> 1s` 的 Kubernetes-oriented
  default，但将 `(0,100ms)` 统一 warning 后 clamp 至 100ms，且 CLI help 明示该语义。新增
  table test 覆盖 negative/zero/sub-min/exact/above；官方 client probe 在 exact image 以
  `--watch-progress-notify-interval=1ms` 启动，要求首个 progress 不早于 75ms 且小于 1s。
  旧 A59 以 1ms 启动时 marker storm 令 setup Put 超过 3s，降至 50ms 后首个 progress
  实测约 62ms，稳定复现未 clamp；table focused 连续 100 轮、相关 backend/Watch race
  连续 10 轮及 full/vet 通过。三副本 TiKV/PD exact image `e4afbbc9dd52` 在 1ms 输入下
  官方 client probe 连续 20 轮通过，且同配置下完整 suite 通过。
- **Watch A61 from-now registration fence（2026-07-17）**：对照 etcd
  `server/etcdserver/api/v3rpc/watch.go` 先调用 `watchStream.Watch` 注册、再发送
  Created control response 的顺序，发现 KubeBrain leader 反向执行：先 `Send(Created)`，
  后由 goroutine 调 `backend.Watch(..., revision=0)`/`AddWatcher`。两步之间提交的事件既不在
  live subscriber 中，也不会被 revision-0 watch 回放，会在不发生断线的正常 stream 上
  永久丢失。现复用 Created header 与 `syncedRev` 的 published watermark，将 leader
  from-now 请求在启动 backend goroutine 前转换为 `watermark+1`；backend 已保证先安装 live
  subscriber 再回放 history，因此交接无缝且不会重复/跳过。新增确定性
  `TestLeaderFromNowWatchReplaysWriteDuringCreatedResponse`，在 Created `Send` callback 内
  提交 watched key，锁定旧实现必丢、修复后必达；官方 client/v3
  `TestFromNowWatchDoesNotLoseImmediatePostCreateWrite` 在三副本 TiKV/PD exact image
  `5c8646871cd1` 上连续 10 轮（500 次 create 后立即 Put）全达，并同时通过 watch-history
  fallback/update 语义。focused 连续 20 轮、相关 race 连续 10 轮及 full/race/vet 通过。
  proxy resume 测试也明确正数 Created header 立即推进 floor，0 仅为旧 server 兼容。
- **Watch A62 filtered batch watermark（2026-07-17）**：对照 etcd
  `server/storage/mvcc/watchable_store.go:watcher.send` 与
  `server/etcdserver/api/v3rpc/watch.go`，etcd 在 filter 去掉部分或全部 event 后仍先推进
  watcher `minRev`，有可见 event 时 response header 使用原始 `WatchResponse.Revision`。
  KubeBrain 原 `WatchResult` 只携带 event，RPC 层 filter 后以“最后一个可见 event”的
  ModRevision 当 header；若全部被 NOPUT/NODELETE 去掉则直接 continue，`syncedRev` 完全不
  前进，导致 RequestProgress 也无法证明已越过被过滤写。现 local backend translator 与
  follower proxy 均显式传递 batch `Revision`；部分可见响应用该 revision，全部过滤则静默
  推进 watermark（不误发 event，也不触发 event 后 progress-elision）。确定性
  `TestFilteredWatchAdvancesThroughFullBatchRevision` 覆盖 full-filter rev 9 静默推进及
  DELETE rev 10 + filtered PUT rev 12 返回 header 12；proxy mapping 也锁定 leader header。
  官方 client/v3 `TestFilteredWatchProgressCoversSuppressedWrites` 在 A61 image 上稳定于 5s
  deadline 前无法覆盖 filtered PUT，A62 exact image `bfbc0b86cc65` 上连续 20 轮（500 次
  NOPUT+RequestProgress）全过，并同时跑 1,000 次 A61 registration、watch-history/update
  场景；focused 50 轮、相关 race 20 轮及 full/race/vet 通过。
- **Lease A63 final-subsecond TTL truncation（2026-07-17）**：对照 etcd
  `server/etcdserver/v3_server.go:leaseTimeToLive` 的
  `int64(le.Remaining().Seconds())`，live lease 最后不足 1 秒时应返回 `TTL=0`，lease 真正从
  lessor 删除后才返回 `TTL=-1`。kind control-plane 的 reference etcd 对 2s lease 实测连续
  多次 `remaining(0s)` 后才 `already expired`。KubeBrain 原 `remainingTTL` 特判
  `deadline still future && truncated==0 -> 1`，客户端只能看到 `1 -> -1`。现删除该向上取整，
  保留过期 duration clamp 到 0。`TestRemainingTTLTruncatesLiveSubsecondToZeroLikeEtcd`
  直接钉死 1.5s→1、0.5s→0、past→0；官方 client/v3
  `TestLeaseTimeToLiveReportsZeroBeforeExpiry` 要求完整观察 `0 -> -1`。A62 exact image 稳定
  复现未见 0，A63 exact image `9afcd402886b` 连续 10 个 2s lease 生命周期全过，并同时回归
  A62 filtered-watch；focused/race 各 100 轮及 full/race/vet 通过。
- **Lease A64 signed explicit IDs vs positive auto IDs（2026-07-17）**：对照 etcd
  `server/etcdserver/v3_server.go:LeaseGrant`（auto ID 对 `reqIDGen.Next()` mask
  `MaxInt64` 并跳过 0）及 `tests/integration/v3_lease_test.go:TestV3LeaseNegativeID`
  （explicit ID 支持 `-1/MaxInt64/MinInt64` 并可恢复）。KubeBrain 原 restore 对每条 lease
  执行 `if id > leaseID { leaseID=id }`；恢复 explicit MaxInt64 后 auto counter 被推到上限，
  下一次 `atomic.AddInt64` 溢出为 MinInt64，违反 etcd auto ID 只为正数且把 explicit/auto
  generator 混为一体。现 restore 不再以 explicit ID 重播 generator；`nextLeaseID` mask
  `MaxInt64` 并跳过 0，极限 wrap 为 1，已有 grant/pending map collision loop 继续兜底。
  `TestAutomaticLeaseIDStaysPositiveAfterMaxExplicitIDReload` 稳定钉死 old code 的
  Max→Min overflow 与 fixed 100→101/Max→1；官方 client/v3 对
  `-1/MaxInt64/MinInt64` 逐一执行 grant/attach/TTL(keys)/revoke，并检查后续 auto positive。
  三副本 TiKV/PD exact image `666d2d7ad04f` 上 signed lifecycle 连续 20 轮；另实际删除持有
  MaxInt64 lease 的 leader pod，replacement leader reload 后 lease/key 均存活、首个 auto ID
  仍 positive/distinct。focused/race 各 100 轮及 full/race/vet 通过，failover 后 3/3
  replacement pods 均 zero restart。
- **Lease A65 atomic revoke metadata deletion（2026-07-17）**：对照 etcd
  `server/lease/lessor.go:Lessor.Revoke`，attached key 与 lease backend metadata 必须位于
  同一个 backend transaction。KubeBrain 原先先以 `TxnApply` 提交 user key 和 attachment
  index 删除，再单独 `InternalDelete` lease metadata；若两次提交之间进程退出，新 leader
  会从残留 metadata 恢复一个已经没有 key 的 lease，且客户端已收到 revoke key 删除。
  现 revoke 与自然过期均把 lease metadata delete 作为 internal op 加入同一个
  `TxnApply`，提交成功后才清理内存 lease；即使 attachment 指向的 user key 已不存在或已换绑，
  也会回收 stale index。注入 combined transaction failure 的单元测试确认 key、metadata、
  TTL 和 attached-key index 全部保留，regrant ordering test 也改为拦截 transaction 内的
  metadata delete。新增官方 client/v3 黑盒测试以 reverse lexical 顺序挂载两个 key，
  要求 revoke response、两个排序后的 DELETE event 共用同一 revision，随后 range 为空且
  TTL=-1。atomic focused 连续 100 轮、timer-heavy focused 连续 50 轮、focused race 连续
  100 轮及 full/race/vet 通过；三副本 TiKV/PD exact image `133c27c01042` 为 3/3 ready、
  zero restart，atomic revoke、signed-ID lifecycle 与 TTL boundary 联合场景连续 20 轮通过。
- **Lease A66 renew/expiry deadline fence（2026-07-17）**：对照 etcd
  `server/lease/lessor.go:Lessor.Renew` 对 `l.expired()` 的检查及等待 revoke 完成的语义，
  发现 KubeBrain 有两个互补竞态：timer callback 已到期但尚未执行时，keepalive 会无条件把
  past deadline 推后，从而复活已过期 lease；反向地，`time.Timer.Reset` 无法撤回已经开始的
  callback，成功 keepalive 后 stale callback 仍会无条件删除刚续期的 lease/key。现
  `refreshLease` 与 revoke/expiry 通过 `leaseWriteMu` shared/exclusive 排序，并在锁内拒绝
  `deadline <= now`；`expireLease` 取得 exclusive lock 后重新检查 deadline，若 keepalive
  已推进则重新 arm timer 并退出。确定性测试分别把 deadline 置于过去后延迟 callback，要求
  keepalive 返回 TTL=0 且不能推进 deadline；以及直接模拟已排队 callback，要求 renewed lease
  保持存活。原 expiry storage-failure test 也改为显式 past deadline，确认失败时 expired
  lease 以 TTL=0 和完整 bindings 等待 retry。官方 client/v3 黑盒测试等待 2s lease 进入仍
  live 的 final-subsecond `TTL=0`，续期后越过原 deadline 再验证 lease/key，避免把 TTL
  truncation 错当 expiry。focused 连续 100 轮、focused race 连续 100 轮及 full/race/vet
  通过；三副本 TiKV/PD exact image `b565fb3b9bbe` 为 3/3 ready、zero restart，renew-boundary、
  atomic revoke、signed-ID lifecycle 与 TTL boundary 联合场景连续 20 轮通过。
- **Lease A67 expired keepalive waits for revoke（2026-07-17）**：继续对照 etcd
  `server/lease/lessor.go:Lessor.Renew` 与 `lessor_test.go` 的 expired-renew test：deadline
  已过后 renewal 必须等待 lease `revokec`，只有 attached key 与 lease metadata 的 revoke
  完成后才返回 not-found。A66 虽已禁止复活，却立即返回 TTL=0；在 timer callback/存储提交
  尚未完成的窗口内，客户端会收到 `ErrLeaseNotFound` 后仍读到 attached key。现每个
  `leaseState` 持有 lifecycle `revoked` channel，grant/recovery 初始化，atomic revoke 成功并
  从内存移除时关闭；expired keepalive 释放 shared operation lock 后等待该 signal 或 stream
  cancellation。只有 NotFound 映射为 TTL=0，context/storage/leadership 等其他错误不再被误吞
  为成功 response。确定性测试确认 response 在 revoke 前阻塞、成功 response 前 key 已删除，
  atomic revoke 注入失败期间继续阻塞并在 retry 后完成，以及 client cancellation 可解除等待
  且不发送 TTL=0。官方 client/v3 黑盒测试同时创建 64 个 2s lease/key，在 deadline 边界并发
  KeepAliveOnce 后立即 Range：positive renewal 必须仍见原 lease key，NotFound 必须已经无 key。
  A66 image 该 burst 连续 10 轮全部复现“not-found before key deletion”；focused 连续 100 轮、
  focused race 连续 50 轮及 full/race/vet 通过。三副本 TiKV/PD exact image
  `ba157b8b9727` 为 3/3 ready、zero restart，burst ordering、final-subsecond renewal 与
  atomic revoke 联合场景连续 20 轮通过。
- **Lease A68 large leased DeleteRange atomicity（2026-07-17）**：对照 etcd
  `server/storage/mvcc/kvstore_txn.go:storeTxnWrite.deleteRange`，一个 DeleteRange 无论命中
  多少 key 都在同一个 `TxnWrite` 中生成 tombstone，并只推进一次 MVCC revision。KubeBrain
  为同步清理 durable lease attachments，把含 leased key 的 range 每 128 个拆成一次
  `TxnApply`；129 key 已出现两次提交、两个 watch revision，且第二 chunk 失败时第一 chunk
  已对客户端可见，破坏 etcd range-delete 原子性。现先对完整 key snapshot 构造 revision
  guards，再把全部 user tombstone 与 attachment internal delete 合并到单次 TiKV transaction；
  commit 成功后才统一更新内存 lease index，空 range 仍返回当前 revision。确定性 129-key
  测试要求 `Deleted/PrevKvs=129` 且 backend revision 仅 `+1`；另在最后一个 key 注入失败，
  要求全部 129 个 user key、TTL attached keys 与首尾 durable attachment 均保留。官方
  client/v3 黑盒测试从 delete 前 revision watch 129-key prefix，要求 Delete response 仅
  `+1`、129 个 DELETE event 共用该 ModRevision、PrevKv lease 正确，随后 range 与 TTL keys
  均为空。A67 image 连续 3 轮稳定复现 response 比预期多推进一个 revision；focused 连续
  20 轮及 full/race/vet 通过。三副本 TiKV/PD exact image `dd1468f93c14` 为 3/3 ready、
  zero restart，258-op atomic transaction 黑盒场景连续 10 轮通过，并追加 expired-keepalive、
  atomic-revoke 与 final-subsecond renewal 联合回归 10 轮。
- **Lease A69 remaining-TTL checkpoint（2026-07-17）**：对照 etcd
  `server/lease/lessor.go` 的默认 5 分钟 checkpoint interval、到期 checkpoint 调度、
  `RemainingTTL` 持久化以及 renew 前清零语义。KubeBrain 原恢复任意 durable lease 都从
  `now+grantedTTL` 重新计时，长 lease 在 leader failover 时即使已接近过期也会被延长完整
  grant。现 leader 每 5 分钟将向上取整的 remaining TTL 写入 internal lease metadata，
  不推进 user MVCC revision；恢复优先采用该 checkpoint，成功 keepalive 在恢复完整 TTL
  deadline 前持久化清零，且 checkpoint、renew、reload/stop 通过独立顺序锁避免旧 metadata
  覆盖新状态。确定性测试将 600s lease checkpoint 为约 240s，验证 reload 保持 granted
  TTL=600、remaining 约 240，renew 返回 600 并清零 checkpoint，前后 user revision 不变；
  reset/reload/checkpoint focused 与 race 各连续 20 轮通过。三副本 TiKV/PD 上 600s lease
  经过真实定时 checkpoint 后删除当前 leader，replacement 返回 granted(600s)、
  remaining(268s)，attached key 仍存在；部署恢复 3/3 ready、zero restart。官方 client/v3
  failover recipe 已固化为 opt-in 长测试；最终 exact image `ea27e6f0c4fe` 通过相邻 lease
  lifecycle 回归。
- **Lease A70 promotion expiry spreading + empty revoke durability（2026-07-17）**：
  对照 etcd `server/lease/lessor.go:Lessor.Promote` 与
  `lessor_test.go:TestLessorRenewExtendPileup`，恢复至少 1,000 个 lease 时，若 reconstructed
  deadline 重叠，etcd 以默认 revoke rate=1,000、目标每秒 75% 将到期窗口向后摊开，避免
  leader failover 后瞬间产生无界 revoke/TiKV delete burst。KubeBrain 原 `applyLeaseRecords`
  对全部记录使用同一个 `now` 并立即 arm timer；现先按 deadline/ID 稳定排序，应用同一
  promotion spreading 算法后再调度 expiry/checkpoint timer。审计真实 1,200-lease failover
  时又发现更基础的 durable gap：空 lease revoke 的 transaction 仅含 internal metadata
  delete，而 `backend.TxnApply` 把“没有 user write”误判为整笔 no-op；leader 内存删除后，
  metadata 仍在 TiKV，下一 leader 会复活该 lease。现 internal-only transaction 仍执行
  fence、guard 和单次原子 batch commit，但不申请 TSO、不推进 user MVCC revision。
  backend 与 lease 确定性测试分别锁定 internal-only delete 落盘、revision 不变，以及
  empty grant→revoke→reload 仍 TTL=-1；promotion helper 以可注入 rate 验证低于阈值不摊开、
  75% 首窗口及每秒上限，focused/race 各 100 轮及 full/race/vet 通过。pre-fix exact image
  `966667196c9c` 的 empty revoke 在真实 active-leader 删除后稳定复活；fixed exact image
  `38a8978f7126` 连续 5 次 leader failover 均保持 TTL=-1/list absent。最终 1,200 个 600s
  explicit lease failover 中，高 ID lease TTL 至少比低 ID 多 1 秒，证明 promotion spread
  可由官方 client/v3 观察；全部测试 lease durable cleanup 后 list=0，部署 3/3 ready、
  zero restart。
- **Lease A71 read demotion fence（2026-07-17）**：对照 etcd
  `server/etcdserver/v3_server.go:leaseTimeToLive` 在读取 lease 后执行的 `le.Demoted()`
  检查，KubeBrain 原先只在 RPC 进入时确认 leader；请求若随后阻塞在 `leaseMu`，并在等待
  期间失去 leadership，仍可能在 `StopLeases` 清空 snapshot 前返回旧 TTL 或旧 lease list。
  现 `LeaseTimeToLive` 与 `LeaseLeases` 在持有最终 snapshot lock 后再次确认 leadership；
  已 demote 时先释放 lock，再按配置转发给新 leader 或返回 `Unavailable`，不再暴露本地
  stale snapshot。lock-controlled 确定性测试同时覆盖 TTL/list，在首次 leader check 后
  切换角色，连续 100 轮及 race 连续 100 轮均稳定拒绝旧读；full/race/vet 通过。官方
  client/v3 黑盒测试以 16 个并发 worker 持续校验 TTL 的 ID、remaining/granted TTL 和
  list membership，并删除 metrics 确认的真实 active leader；exact image
  `8cfd0200222d` 连续 5 轮 failover、共 85.244s 未返回任何 stale-shaped success。
  相邻 lease lifecycle 回归通过，最终部署 3/3 ready、zero restart，cleanup 后 list=0。
- **Lease A72 authorized read snapshot（2026-07-17）**：对照 etcd
  `server/etcdserver/v3_server.go:checkLeaseTimeToLive`、`leaseTimeToLive` 与
  `checkLeaseLeases` 的 key 级 READ 检查和 auth revision fence，审计发现 KubeBrain
  原先先从一个 `leaseMu` snapshot 收集 key 并鉴权，释放 lock 后再从后一个 snapshot
  构造 `TTL(Keys=true)`/`Leases` response。并发 leased Put 可在两者之间附着受保护
  key，使成功 TTL response 返回从未鉴权的 key；List 的授权 gate 与枚举也不属于同一
  linearization point。现最终 leadership check、完整 key 鉴权、auth revision fence 和
  response 构造全部在同一次 `leaseMu` acquisition 内完成；新 attachment 要么参与本次
  权限检查，要么在线性化上发生于成功 response 之后。确定性 backend shim 在 revision
  fence 暂停 read，并让受保护 leased Put 的 TiKV transaction 先 durable commit、再等待
  内存 index publish；pre-fix `37dee63` 的 TTL/List 两个子测试均稳定失败，修复后连续
  100 轮通过，race 连续 20 轮通过，full/race/vet 全部通过。exact image
  `0332f016168c` 在三副本 TiKV/PD 上执行 5 轮临时 auth-enable 黑盒 race（16 个
  client/v3 TTL reader 持续对抗 root attach/detach）共 48.964s 无 protected-key
  disclosure；追加 verbose 轮验证 6,052 个成功 TTL snapshot 和 70 个受保护绑定周期。
  相邻 lease 回归连续 3 轮通过；最终 auth disabled、临时 principal 清理、lease list=0，
  部署 3/3 ready、zero restart。
- **Lease A73 authorized renewal fence（2026-07-17）**：对照 etcd
  `server/etcdserver/v3_server.go:checkLeaseRenew` 的逐 key WRITE 权限检查、auth
  revision fence 与 `server/lease/lessor.go:Renew` 的 deadline/checkpoint 更新，
  审计发现 KubeBrain KeepAlive 原先先鉴权，再进入 `refreshLease` 的
  `leaseWriteMu.RLock`；普通 leased Put 同样持 shared lock，因此可在鉴权和续租之间
  提交受保护 key，令普通用户延长其无权写入的数据生命周期。现非 root 鉴权续租按
  `leaseCheckpointMu -> leaseWriteMu.Lock` 顺序，把当前 key snapshot 鉴权、auth
  revision fence、checkpoint 清零及 deadline 更新放在同一个 write exclusion window；
  auth disabled 和 root 续租仍走 shared lock 快路径，不把常规 KeepAlive 全局串行化。
  expired lease 在等待 revoke completion 前释放两把锁，避免与 expiry/revoke 互锁。
  确定性测试暂停 KeepAlive 的 auth revision fence，并并发执行真实 root leased Put：
  pre-fix `e0a2d2c` 稳定出现 attachment transaction 先提交，修复后 Put 必须等待续租
  鉴权完成；联合 checkpoint、TTL=0 和 permission-revoke 场景普通测试连续 100 轮、
  race 连续 20 轮以及 full/race/vet 全部通过。三副本 TiKV/PD exact image
  `5e4301154c42` 上，16 个 TTL reader、8 个 KeepAlive worker 与 root attach/detach
  混合压力连续 5 轮、共 51.148s 通过；追加 verbose 轮完成 5,716 个成功 TTL
  snapshot、123 次成功续租、367 次预期鉴权拒绝和 70 个保护绑定周期，无泄露、死锁
  或停滞。相邻 KeepAlive/revoke/TTL 回归连续 3 轮通过；最终 auth disabled、临时
  principal 清理、lease list=0，部署 3/3 ready、zero restart。
- **Txn A74 uncertain commit atomic resolution（2026-07-17）**：TiKV
  `ErrResultUndetermined` 会映射为 `storage.ErrUncertainResult`。原 `TxnApply` 将一笔
  多键事务的 invalid events 逐键送入 async FIFO；若原 TiKV batch 实际已提交，队列会
  以不同的新 revision 重写各键，破坏事务原子性。pre-fix `50c94f6` 的确定性存储注入
  稳定复现两个键分别变为 original+1 等后续 revision。现 uncertain 多键事务不再进入
  单键修复队列，而是读取与用户 mutation 同批原子写入的 event-log marker：全部存在即
  按原 revision 一次发布整批 watch events，全部不存在即一次跳过该 revision，mixed
  或暂时性读取错误则以 100ms 到 1s 的有界指数退避重试整笔事务，不猜测结果。
  committed/not-committed 两条确定性路径均覆盖，其中 committed 路径先注入 3 次
  `ErrUnavailable`；聚焦普通测试 100 轮、race 20 轮以及 full/race/vet 全部通过。
  三副本 KubeBrain + 独立 TiKV/PD exact image
  `cdb642a82cc0ad3f950375a13577bf9aba2253772c0a75f629b04f40095d3f08`
  上，无故障 Porcupine 多键历史连续 5 轮通过；两次删除唯一 TiKV Pod 分别产生 3 和
  2 个 ambiguous Txn 结果，两次完整历史均为 `Ok`。相邻五类事务原子性、staged view
  与 version 语义黑盒各连续 10 轮通过；最终 TiKV 与部署均恢复 Ready。第二轮故障中
  active leader 超过 5s renew deadline 后按既有 watch-cache self-fence 策略 exit 255，
  Kubernetes 重建 1 次，非探针失败或事务 resolver 崩溃。
- **Lease A75 uncertain attachment reconciliation（2026-07-17）**：A74 已能判定
  uncertain `TxnApply` 的 durable 结果并恢复 watch，但 etcd 层只在 `TxnApply` 返回成功
  后更新 `keyLeaseIndex`。若 leased Put 的 value + attachment 已提交却返回 uncertain，
  当前 leader 会漏记该绑定，随后 revoke/expiry 不会删除该键，直到 leader reload 才
  自愈。现 backend shim 在错误路径保留 reserved revision；普通 leased Put、atomic
  generic Txn、staged Txn 和带 attachment 的 DeleteRange 遇到
  `storage.ErrUncertainResult` 时，等待 collector committed/skip 该 revision，再仅针对
  本批实际包含 attachment mutation 的用户键读取 durable record。对账持
  `leaseWriteMu.Lock`，与后续 attach/detach/revoke/expiry 排序，并在读取失败时以
  100ms 到 1s 指数退避整组重试；节点已失去 leadership 则退出，由新 leader reload
  权威状态。确定性测试让真实 memkv batch 先提交 value+attachment、调用层再收到
  uncertain，并连续注入 3 次 attachment `ErrUnavailable`；索引最终恢复，紧随其后的
  revoke 同时删除 user key 和 attachment。聚焦普通与 race 各连续 20 轮、
  full/race/vet 全部通过。三副本 TiKV/PD exact image
  `31165d4ff79b7c361cb469b9d2a204c392962ddc47faae1215885b32175e6763`
  上 lease Porcupine 基线连续 5 轮及 TiKV Pod 删除恢复历史均为 `Ok`；故障轮未捕获
  ambiguous lease RPC，因此仅作为恢复回归，uncertain 分支以确定性注入为主证据。
  相邻 lease history 再连续 3 轮通过；参考 etcd differential 因本轮未启动 reference
  endpoint 明确跳过，不计为通过。
- **Deploy A76 production pod hardening（2026-07-17）**：审计生产清单发现 TiKV/PD
  已配置 requests/limits、required anti-affinity 和 PDB，但 KubeBrain 容器仍无资源
  约束，副本反亲和仅为 preferred，且默认挂载 ServiceAccount token、缺少 pod/container
  security context。明文与 TLS 两份清单现统一要求 3 个不同 hostname（生产集群至少
  3 个可调度节点），为 KubeBrain 设置 requests `500m/1Gi`、limits `2CPU/4Gi`，
  ServiceAccount 与 Pod 双层禁用 token automount，并固定 UID/GID 65532、
  `runAsNonRoot`、`RuntimeDefault` seccomp、只读根文件系统、禁止 privilege escalation
  及 drop `ALL` capabilities。结构化 manifest 测试锁定全部约束并连续 20 轮通过；
  两份清单均通过 `kubectl apply --dry-run=client`。exact image
  `31165d4ff79b7c361cb469b9d2a204c392962ddc47faae1215885b32175e6763`
  在 `--read-only --user 65532:65532 --security-opt no-new-privileges --cap-drop ALL`
  下正常启动 CLI；full test 通过。required anti-affinity 有意未部署到单节点 kind，
  其不可调度正是对错误生产拓扑的 fail-closed 行为。
- **Storage A77 cross-engine BatchWrite contract（2026-07-17）**：重新核对维护性
  审计 C1，确认共享 `storagetest.RunBatchWriteContract` 已接入 memkv、Badger 和 opt-in
  TiKV，但缺少计划中明确列出的 `DelCurrent` 版本语义。现同一套件新增 iterator snapshot
  未变化时成功删除，以及 snapshot 后 key 被覆盖时必须返回 `ErrCASFailed`、保留 replacement
  两条契约；连同既有 PutIfNotExist、CAS missing/mismatch、Put/Del 和 conflict batch
  atomicity 共 10 条。memkv/Badger 各连续 20 轮通过；测试二进制作为临时 Pod 在 kind
  集群原生 DNS/网络内直连独立 TiKV/PD，完整套件连续 10 轮通过。TiKV uncertain commit
  映射由 A74 的 committed/not-committed 注入继续覆盖。同期启动 `/root/etcd` 3.8 alpha
  独立参考实例，Compact、DeleteRange、HashKV、Lease、自然过期、MemberList、Put、
  Range/RangeStream、Serializable Read、三类 Txn 和 WatchControl differential 全部
  通过；JWT 需要独立 JWT 配置端点，按测试前置条件跳过，不计为通过。
- **Storage A78 wrapped capability discovery（2026-07-17）**：重新核对维护性审计
  C2，确认早期修复虽已让 metrics wrapper 直接保留 `GarbageCollector` 和
  `ExclusiveKvStorage`，但后来新增的 `ClusterIdentifier`、`BatchGetter` 会被包装层
  静默隐藏：前者令 etcd response header 退回 keyspace hash 而非 PD cluster ID，后者
  令 event-log replay 从 TiKV BatchGet 退化为逐键 Get。现 storage 定义统一的 decorator
  unwrap 契约和带 32 层上界的泛型 `FindCapability`；metrics wrapper 可解包，backend
  cluster ID、event-log batch read、storage GC 和 scanner exclusive client 四类生产
  消费点全部统一发现能力，同时保留 GC/ExclusiveKvStorage 既有直接类型断言兼容，避免
  为能力组合维护 O(2^n) wrapper 类型。两层 metrics wrapper 与异构 decorator 测试覆盖
  四类能力及“不凭空生成能力”，普通与 race 各连续 20 轮、full/race/vet 全部通过。
  exact image
  `b808cfca3b291b3dc2af353757d87dbbfc314a66c14c9e2ac0287b42c7ec6e8e`
  在三副本 KubeBrain + 独立 TiKV/PD 上以
  `--enable-storage-metrics=true` 滚动部署后 3/3 Ready、zero restart；etcd
  `Status.header.cluster_id=7662961163671170154` 与 TiKV client 从 PD 取得的 cluster ID
  完全一致，实时 Put/Get/Delete 通过。
- **Admission A79 instance-wide client concurrency（2026-07-17）**：现有
  `--max-concurrent-streams` 与 etcd 一致只约束单条 HTTP/2 connection，多连接流量可
  绕过它并耗尽一个 KubeBrain 实例。新增 `--max-requests-inflight`（默认 `0` 保持 etcd
  无全局上限，生产清单显式 `1024`），统一计算公开 client listener 上 unary 与完整
  stream 生命周期的总并发；超限返回 etcd 已定义的
  `ErrGRPCRequestTooManyRequests`（`ResourceExhausted: etcdserver: too many requests`）。
  peer listener 继续执行 request-size 检查和 response header stamping，但不消耗公开
  client 槽位，为 leader/revision 协调及内部转发保留容量。新增 inflight gauge 与按
  method/kind 标记的 rejected counter。两个独立连接、长 stream、disabled fast path、
  peer reserve 和生产 manifest 测试普通/race 各连续 20 轮，full/race/vet 全部通过。
  exact image
  `035ce26167b8b41ceccdeddeef7778fa4dafabdfa3e5b216bd0c93f090e96a04`
  在三副本 KubeBrain + 独立 TiKV/PD 上以临时 limit=1 验证：同一 Pod 的 client watch
  占槽后，第二连接 Status 稳定收到标准 ResourceExhausted，指标显示 inflight=1、
  rejected method=`/etcdserverpb.Maintenance/Status`；同时 peer listener 的 Status
  成功并返回正确 PD cluster ID。取消 watch 后 inflight 回零且 client health 成功。
  最终恢复 limit=1024，部署 3/3 Ready、zero restart。
- **Watch A80 instance-wide logical watch quota（2026-07-17）**：一个 Watch gRPC
  stream 可 multiplex 任意数量逻辑 Watch，因此 A79 的 stream 生命周期槽位不能约束
  单连接内的 Watch 数。新增 `--max-watches`（默认 `0` 保持既有/etcd 无逻辑总量上限，
  生产清单显式 `10000`），按进程原子计算所有 stream 的 active Watch；超限 create 返回
  etcd 风格的 `Created=true,Canceled=true,WatchId=-1,CancelReason="etcdserver: too many
  requests"`，不终止同一 multiplexed stream。显式 cancel、后端 cancel 和 stream
  disconnect 均只释放一次；真实压力验证发现 `watcher.Close` 原先依赖后端代理协程退出
  才清理 map，现关闭路径先同步清表并归还配额，再等待协程收尾。另在滚动切主时发现新
  leader 的 current/event-log 起点已在高 revision、published watermark 却仍为 0，
  from-now Watch 会从 revision 1 回放并立即 compact；leadership 初始化现先关闭旧任期
  subscribers，再把 published 安全推进到 fresh storage revision。逻辑配额、非法/重复
  create、跨 stream 原子接纳、同步断连释放和 leadership watermark 测试普通/race
  高频通过，full/race/vet 全部通过；官方 protobuf Watch client 黑盒在三副本 KubeBrain
  + 独立 TiKV/PD、临时 limit=1 下连续 50 轮完成接纳、拒绝、取消、槽位复用和最终清理，
  指标精确为 rejected=50、active=0。exact image
  `f03aa369bba116007f5bec340ccf5fa98aabd33390e33365e0d1e6a74b5518e3`；
  最终恢复 limit=10000，部署 3/3 Ready、zero restart，实时
  `endpoint health` 与 Put/Get/Delete 通过。
- **Admission A81 client request-message rate limit（2026-07-17）**：A79 仅限制
  在途 RPC，短 unary 洪峰可在并发槽释放后继续无限进入；若只按 stream 建立扣 token，
  单条 Watch/LeaseKeepAlive 双向流也可无限发送消息。新增成对 fail-loud 参数
  `--max-request-rate`/`--request-rate-burst`（默认均 `0` 保持 etcd 无公开 QPS 配置的
  行为，生产清单为每进程 `2000 msg/s`、burst `4000`），使用并发安全 token bucket：
  unary 每 RPC、stream 每入站 message 各扣一个 token，超限统一返回 etcd 标准
  `ResourceExhausted: etcdserver: too many requests`，并按 method/kind 记录
  `grpc_server_rate_limit_rejected`。真实 follower 验证同时发现 etcd proxy 仍拨 leader
  client 端口，导致外部请求在入口和 leader 重复消耗 QPS/A79 并发预算；现 proxy 改拨
  election identity 的 peer 端口并使用独立 peer TLS/ServerName，leader 内部执行不再进入
  public admission。peer listener 本就注册完整 RPC surface，生产网络必须仅允许副本访问。
  CLI/config/TLS 传播、成对校验、unary burst/refill、64 并发原子争抢、同 stream 多消息、
  peer reserve 测试普通 100 轮、race 50 轮及 full/race/vet 全部通过。三副本 KubeBrain
  + 独立 TiKV/PD exact image
  `61c62b1cfbe4162cff56b28fb93731a7afb083fb28d4e4c6919eb8b5dfdad58a`
  上，明确连接 follower、临时 rate=1/burst=2 连续 5 轮：每轮 3 个并发 unary 恰好
  2 成功/1 拒绝，peer health 同时成功，refill 后 client 恢复；同一 Watch stream 前两条
  create 经 peer 代理成功、第三条 message 标准拒绝。指标精确为 unary rejected=5、
  stream_message rejected=5、inflight=0。最终恢复 2000/4000，部署 3/3 Ready、
  zero restart，实时 endpoint health 与 Put/Get/Delete 通过。
- **KV A82 atomic large DeleteRange（2026-07-17）**：对照 etcd
  `server/etcdserver/v3_server.go:DeleteRange` 的单次 Raft apply 语义，发现 KubeBrain
  非租约范围删除每 128 key 分块提交，导致一个请求产生多个 revision，后续 chunk
  失败时还会暴露部分删除；租约路径虽已原子化，但普通 key 与事务内 DeleteRange
  仍可走旧路径。现所有范围删除统一通过一个 `TxnApply`，用户 tombstone、watch event
  与 lease attachment 在同一 TiKV 事务和同一 MVCC revision 提交，不确定提交沿用事务级
  解析，不再逐 key 补偿。新增 `--max-delete-range-keys`（默认 `0` 保持 etcd 不限数量，
  生产清单 `1024`）：启用后以 `limit+1` 的有界扫描在写入前拒绝超大范围，返回标准
  `ResourceExhausted: etcdserver: too many requests`，不消耗 revision；指标
  `delete_range_admission_rejected` 可用于发现调用方误用。单元测试覆盖 300 key 后端
  单 revision、129 个无租约 key 的尾部故障全回滚、上限有界扫描与零写入，并覆盖
  CLI/config/manifest 传播。full test、受影响包 race 与 full vet 均通过；官方
  `client/v3` 在三副本 KubeBrain + 独立 TiKV/PD、临时 limit=256 下验证：129 key
  DeleteRange 返回 129 个 PrevKV 且只推进一个 revision，257 key 的普通与 Txn
  DeleteRange 均标准拒绝，revision 不变且 257 key 全部保留，指标精确增加 2。
  exact image
  `4584b5ed9623b56b8af8df46ec11d1e7c09618d9236342d661ecbf593646f440`；
  最终恢复 limit=1024，部署 3/3 Ready、zero restart，实时 health 与 CRUD 通过。
- **Txn A83 caller deadline propagation（2026-07-17）**：对照 etcd
  `server/config/config.go:ReqTimeout` 与 `etcdserver/v3_server.go`，请求预算应覆盖
  排队、计算、磁盘 I/O 和可能的 leader election，并由 request context 贯穿 apply。
  KubeBrain etcd 层虽给 Txn 10 秒预算（且会被更短客户端 deadline 截断），backend
  `TxnApply` 却固定在 1 秒停止 CAS 重试，且 standalone Put/DeleteRange 未主动补
  server budget，导致 TiKV leader transfer、region
  reschedule 或短暂热点冲突在有效请求预算内被提前误报 `Unavailable`。现
  Put/DeleteRange/Txn 和 Lease Grant/Revoke unary 写入口统一注入 10 秒预算并保留
  更短 client deadline；`TxnApply` 使用该 context deadline。仅直接/后台无 deadline
  调用保留 1 秒兜底，避免无界循环。确定性存储故障测试让 CAS 冲突持续 1.1 秒：
  2.5 秒 caller deadline 下恢复后成功原子提交；background context 仍约 1 秒返回
  `ErrUnavailable`。server 记录型 shim 另验证 standalone DeleteRange 无 client
  deadline 时收到约 10 秒预算，500ms client deadline 不会被延长。focused
  普通 10 轮、race 3 轮及 full test/race/vet 全部通过；三副本 KubeBrain +
  独立 TiKV/PD 上官方 client/v3 的多写 Txn、compare Txn、staged range、
  overlapping DeleteRange 单 revision 与 Lease+physical compact 五组连续 3 轮
  通过。exact image
  `e51405856ed4e7457f42912a5b43595c960c29cc4b0e22ef3df7c179243d6dc6`；
  最终部署 3/3 Ready、zero restart，稳定期日志、health 和 CRUD 正常。
- **Watch A84 slow-consumer control-plane isolation（2026-07-17）**：对照 etcd
  `server/etcdserver/api/v3rpc/watch.go` 的独立 `sendLoop` 与 16 深度
  `ctrlStream`，发现 KubeBrain 虽已有每 subscriber 10k batch buffer、满载后从
  100k event ring 无缝追赶/重新挂接、ring 淘汰后明确关闭等数据面保护，但 stream
  Recv loop 仍直接争用 gRPC `Send` 锁。客户端停止读取、event send 阻塞时，cancel
  与 progress 响应也会阻塞 Recv loop，导致已发送的取消请求不能及时释放逻辑 Watch
  配额。现每 stream 增加 16 深度有界 control queue 与单独 sender：created 响应仍
  等待真实发送成功后才启动 backend Watch，确保 created-before-events；事件发送仍
  同步确认后才推进 `syncedRev`；cancel/progress 可在慢 event send 后排队，接收循环
  可继续处理控制请求。确定性测试阻塞首个 event send，验证 control enqueue 不阻塞，
  释放后仍按 event→control 串行发送；既有 hub 测试继续覆盖无缺口 catch-up、ring
  淘汰关闭、删除期间追赶及连续前缀。full test、server/backend race 和 full vet
  通过。三副本 KubeBrain + 独立 TiKV/PD 上官方 client/v3 的 Kubernetes
  watch+lease、历史回放、过滤 progress、事件后 progress 抑制、from-now 创建窗口和
  update/prevKV 六组连续 3 轮通过；完整通用 smoke 也通过。exact image
  `e01d0161779236645fe0014f84e871f13c74c9d7b7a9109ead22ae7f3ec51c9f`；
  最终部署 3/3 Ready、zero restart，稳定期无 panic/fatal/control-send error，
  health 正常。
- **Harness A85 revision-filter Count contract（2026-07-17）**：A84 真实环境首次
  完整 smoke 在 `WithMinModRev` 断言处失败。对照 etcd
  `server/etcdserver/txn/range.go:assembleRangeResponse` 与上游
  `tests/common/kv_test.go` 后确认服务端无回归：etcd 的 `RangeResponse.Count`
  是 revision filter 前 range 的总键数，filter 只裁剪 `Kvs`，因此 4 个键过滤为
  1 个时应为 `Count=4,len(Kvs)=1`；`CountOnly+WithMaxModRev` 同样保留总数 4。
  修正 smoke 中错误的 1/3 预期并写明契约。修正后完整官方 client/v3 smoke 在 A84
  三副本 TiKV/PD 环境通过。
- **Compact A86 physical cancellation recovery（2026-07-17）**：对照 etcd
  `server/etcdserver/v3_server.go:Compact` 与
  `server/storage/mvcc/kvstore.go:Compact`，logical compact 在 apply 时先持久化，
  physical 请求等待该 apply 已调度的清理完成；客户端断开不会撤销已提交的清理目标。
  KubeBrain 的同步 `Physical=true` 路径同样先持久化 logical watermark，但物理扫描
  直接使用请求 context；deadline/cancel 或瞬态 partition discovery 失败后只向客户端
  返回错误，没有把已持久化目标交给后台 worker，可能永久留下 logical watermark 已前进、
  physical history 未回收的状态，直到更高 revision compact 或进程重启。现
  `physicalCompact` 失败后立即把同 revision 调度给 client-independent compactor；
  RPC 仍如实返回原错误，后台沿用既有失败重试与最高目标合并机制。确定性测试分别注入
  partition failure 和阻塞到 `context.Canceled` 的首次扫描，确认 logical watermark
  单调、请求错误不被吞掉，解除故障后 `compactDoneRev` 必须自行达到目标；普通 50 轮、
  race 10 轮及 full test/backend+server race/full vet 均通过。Compact 与参考 etcd 的
  boundary/repeated/older/future/negative/header 差分连续 5 轮一致。三副本
  KubeBrain + 独立 TiKV/PD 上官方 client/v3 `Physical=true` 与并发 point/range/write/
  watch 测试连续 3 轮通过，每轮 100/100 事件完整，compact boundary 可读且 boundary-1
  标准拒绝。exact image
  `875b570a9008cd25f6763a95731961bd43625db2849b64f1c5e44774b5734e11`。
- **Compact A87 shared-TiKV leader ownership（2026-07-17）**：etcd 每个 member
  清理自己的 bbolt；KubeBrain 三副本共享同一 TiKV keyspace，因此 physical GC
  必须只有当前 leader 执行。审计发现启动恢复虽仅在 `onStartedLeading` 调用，
  `runCompactor` 却使用永久 `context.Background()`；旧 leader 失去任期后仍可继续
  扫描，新 leader 同时从持久化 watermark 恢复，造成重复全量 GC/I/O 放大。同步
  Compact 入口也只检查瞬时 `IsLeader`，未像 Put/Delete/Txn 一样携带 epoch，旧 leader
  可在准入后推进共享 logical watermark。现 `ResumePhysicalCompaction` 注册
  leader-election lifecycle context，后台每轮扫描使用该 context，失去任期立即取消；
  新任期注册新 context 并唤醒同一 durable target。Compact RPC 改用
  `EpochAndLeadingFresh` 准入，logical watermark CAS 前通过 `fenceAdmit` 复核 epoch，
  stale term 映射标准可重试 `Unavailable`；auto-compactor 等无 RPC context 的内部调用
  会主动捕获当前 epoch，关闭 ticker-check 到 CAS 的 TOCTOU。测试覆盖旧 context 扫描
  取消后不得完成、新 context 无新增 logical request 即接续完成，RPC epoch 变化不创建
  compact marker，以及内部 CompactAsync 捕获 epoch 后被提交前切主 fence；普通 50 轮、
  race 10 轮及 full test/backend+server race/full vet 全通过。三副本 TiKV/PD 上
  Physical=true 并发 100 次读写/watch 连续 3 轮通过；compact fault smoke 在 30 轮
  compact、4 readers/723 reads 中删除并重建一个副本后完成。exact image
  `315604ad8d076af06e9d38220a8c0993004dd1a1632f9281279e8cab1c3191d2`。
- **Storage GC A88 leadership lifecycle fencing（2026-07-17）**：KubeBrain 在独立
  TiKV/PD 上承担 `gc_worker`，必须由当前 leader 单独推进全局 service safepoint。
  审计发现 driver 虽在每个 tick 前检查 `leadingFresh`，实际 TiKV GC、锁解析和 PD
  safepoint 更新却使用 `context.Background()`；旧 leader 在检查后失去任期时仍可继续
  执行昂贵 GC，新 leader 又会启动下一轮。现复用 A87 的 leadership lifecycle context，
  并在 `onStartedLeading` 最前面注册该 context，使 storage GC 和 physical compaction
  均在 lease/event 初始化期间也受任期取消约束；每轮 GC 继续保留 interval timeout。
  确定性测试阻塞旧 leader 首轮 GC，确认任期 context 取消会中断调用、follower 不推进
  safepoint，新 leader 注册新 context 后无需重启 driver 即可接续；focused 普通 50 轮、
  race 10 轮及 full test/backend+server race/full vet 全通过。三副本 KubeBrain +
  独立 TiKV/PD 临时使用 5 秒周期时，仅 leader `l9gpj` 推进 safepoint；删除该 leader
  后由 `pgvcf` 单独接管，PD safepoint 从 `467743587573956608` 推进至
  `467743598322122752`。验证后恢复默认 10 分钟周期，部署 3/3 Ready、zero restart，
  health 正常。exact image
  `7a78f3eed5773e4224b34d4217f33586cb432f1df25581fa3d6d3d7ba450b97e`。
- **Lease A89 orphan sweeper leadership fencing（2026-07-17）**：对照 etcd
  `server/lease/lessor.go` 的 `demotec`（primary demote 时关闭，终止
  `runLoop`）和 `server/etcdserver/server.go:revokeExpiredLeases`，leader-only lease
  清理不得越过任期。KubeBrain 的 orphan attachment 安全网虽在每个 key 前检查
  `IsLeader`，但整个共享 TiKV 扫描使用 `context.Background()`，且 attachment/key
  删除未携带准入 epoch；旧 leader 可在检查后切主并继续扫描或提交。现
  `ReloadLeases` 将 leader-election lifecycle context 交给 sweeper，在途读取随任期
  取消；每轮及每个 attachment 使用 `EpochAndLeadingFresh` 捕获 epoch，所有后续
  user-key compare-delete 和 internal attachment delete 均由 backend commit fence
  复核。ticker 与取消同时就绪时也优先退出，避免 demote 后的无意义扫描和错误日志。
  确定性测试分别阻塞首次 `InternalRange` 验证 context 取消，以及阻塞
  `InternalDelete` 后切换 epoch，确认旧任期不能回收 attachment；focused 普通 50 轮、
  race 10 轮、full test/backend+server race/full vet 全通过。三副本 KubeBrain +
  独立 TiKV/PD 上，TTL=8 秒 leased key 创建后删除 leader，新的 leader 接管过期，
  watch 仅收到一次 DELETE，key 与 lease 最终均不存在；另以官方 client/v3 连续 3 轮
  验证每轮 50 leases 的 50 PUT/50 DELETE 全量事件。部署 3/3 Ready、zero restart，
  health 正常。exact image
  `9855b01b1e5c54da8313cc7e51d2019ca25fd2e44bd1d7a6a91517044cfa80ea`。
- **Lease A90 reload migration epoch fencing（2026-07-17）**：A89 后继续审计
  leadership acquisition，发现 `ReloadLeases` 的 pre-A17 legacy migration 虽使用
  leader lifecycle context，却没有写入准入 epoch。旧 leader 可从 legacy 快照生成
  internal meta/attachment，在切主后与新 leader revoke 交错并晚提交，导致已撤销 lease
  被下一次 reload 复活。现 reload 入口必须通过 `EpochAndLeadingFresh`，follower 或
  stale leader 直接返回标准 `Unavailable`；捕获的 epoch 贯穿读取、legacy attachment/
  meta 写入和旧 user-MVCC 记录退休，统一由 backend commit fence 复核。确定性测试在
  首个 migration `InternalPut` 前阻塞并切换 epoch，确认旧任期不能生成 replacement
  meta/attachment，legacy 源记录仍保留供新 leader 幂等重试；另验证 follower reload
  无存储副作用地拒绝。focused 普通 50 轮、race 10 轮、full test/backend+server race/
  full vet 全通过。三副本 KubeBrain + 独立 TiKV/PD 上创建 20 个 TTL=8 秒 lease 后删除
  leader，新 leader reload/接管过期成功，官方 client/v3 watch 完整收到 20 PUT/20
  DELETE。部署 3/3 Ready、zero restart，health 正常。exact image
  `5d8f0cd29a98deaf691a9b84c43dfccff83d27406a736dece66c0d3f422a03e6`。
- **Lease A91 uncertain reconciliation generation fencing（2026-07-17）**：对照
  etcd `server/lease/lessor.go` 由 `demotec` 隔离 primary 任期、`Promote` 重建 lessor
  状态的生命周期，审计 KubeBrain committed-uncertain Txn 的异步 lease-index 修复时
  发现：goroutine 仅在 durable attachment 读取前检查 leader，且 `StopLeases`/
  `ReloadLeases` 不与它共用 `leaseWriteMu`；旧任期读取可在新 leader reload 后晚写回，
  清除或覆盖新 generation 的 binding，导致 TTL/LeaseTimeToLive 内存视图与 TiKV
  attachment 不一致。现创建修复任务时捕获 leadership epoch 与 `leaseGeneration`，
  等待 revision、开始读取及写回前均复核 epoch；最终在 `leaseMu` 内复核 generation
  并批量应用，使 Stop/Reload 与旧任务写回严格互斥。确定性测试让旧任务先读到
  attachment=A 后阻塞，切换 epoch、替换 durable lease/attachment 为 B 并完成 reload，
  放行后确认 binding 始终为 B；既有 committed-uncertain leased Put/revoke 测试继续
  通过。focused 普通 50 轮、race 10 轮、full test/backend+server race/full vet
  全通过。三副本 KubeBrain + 独立 TiKV/PD 上，100 个 TTL lease 切主后最终全部清理；
  failover-aware 官方 client/v3 测试进一步创建 1200 leases、删除 leader并等待 rollout，
  验证新 leader reload 后 expiry pileup 正确分散并完成清理。exact image
  `3abc3179684de0031bf439f49976cf9a18f84f2aeb77432e6639ec1f22928728`。
- **Write A92 uncertain retry leadership fencing（2026-07-17）**：对照 etcd
  `server/etcdserver/v3_server.go:raftRequest`，写请求必须在当前 Raft leadership 下
  propose，随后由各 member apply 到各自本地存储；KubeBrain 多副本却共享 TiKV，
  follower 不能独立创建 repair revision。审计发现 single-key uncertain result 的
  `asyncFifoRetry` 永久使用 background context：旧 leader 的队列在 demote 后仍可能
  Deal 新 revision、CAS 重写 key 并写 event log。即使 revision CAS 防止覆盖较新的值，
  key 未变化时仍会产生一个新 leader live collector 未收到的 committed-yet-unwatched
  写。现每轮 retry 在读取/Deal 前通过 backend 捕获 fresh leadership epoch，follower
  保留队列且不分配 revision；打开 batch 前由 `fenceAdmit` 复核同一 epoch，关闭
  admit→commit TOCTOU。fence 拒绝发生在 Deal 后时沿用既有 invalid dispatcher 填充
  revision slot 并重新排队，不留下 collector hole。确定性测试制造需要 rewrite 的
  invalid event，确认 follower 阶段 key revision 不变、队列保留，新 epoch leader
  自动消费并提交 repair；既有 create/update/delete uncertain rewrite 全覆盖继续通过。
  focused 普通 30 轮、race 10 轮、full test/backend+server race/full vet 全通过。
  三副本 KubeBrain + 独立 TiKV/PD 上 Porcupine register history 在并发 Put/Txn 期间
  删除 leader，5 个 transport/fence 失败作为 ambiguous outcome 建模后仍判定
  linearizable。部署 3/3 Ready、zero restart，health 正常。exact image
  `e0016a477626e829f38ab4b7064bc9ea6b39a6549600c7286dbf39ac2d9ee7a6`。
- **Runtime A93 coordinated graceful shutdown（2026-07-17）**：对照
  `/root/etcd/server/embed/etcd.go:Close` 先停止接入、再停止核心循环、最后等待
  子 goroutine 的顺序，补齐 KubeBrain 此前缺失的统一进程生命周期。backend 的
  collector、revision durability、count metrics、compactor、auto-compactor、
  physical compaction 和 storage GC 七类常驻 worker，以及 uncertain txn repair
  动态任务，现在统一注册到可取消 context 与 WaitGroup；lease orphan sweeper、
  uncertain index reconciliation 和 expiry/checkpoint timer 任务也使用独立的
  lease lifecycle。`Close` 先阻止新任务注册，再取消并等待在途任务，最后关闭
  TiKV storage，避免 worker 在 client close 后继续读写；watch stream 同时响应
  shutdown context。Endpoint 按 server/lease tasks → backend workers → TiKV/PD
  client 的顺序幂等关闭，并把 HTTP/gRPC/listener 的正常 close 归一为成功，修复
  `runSubServer` 和 root mux goroutine 共享命名错误变量的 data race。阻塞式 GC
  回归确认 storage close 严格发生在 worker 退出后且只执行一次；focused 普通
  20 轮、race 20 轮、full test、backend/server/etcd/endpoint full race 与 full
  vet 均通过，最终日志改动后 endpoint race 20 轮和 full vet 再次通过。三副本
  KubeBrain + 独立 TiKV/PD 上分别删除 follower 和 leader，应用关停约 33ms/35ms，
  最终镜像复验约 36ms；listener/endpoint 均以 Info 正常退出，TiKV/PD client
  完整关闭，无 Cobra usage、panic 或强制退出。client-go leader election 在 context
  取消与锁读取相撞时仍会先输出一条其内部 Error，随后明确记录 cancellation stop，
  不代表关停失败。部署自动恢复 3/3 Ready，health 与 CRUD 正常。exact image
  `d708d8582c22a2c1b3738b388925aef31e432ba5c95a69c90429639952f4a150`。
- **Runtime A94 graceful TLS shutdown（2026-07-17）**：A93 的真实集群验证仅覆盖
  明文 listener；对照 etcd `server/embed/serve.go` 的 serve context 与
  `client/pkg/transport/listener.go` 的握手时证书重载后，补测发现 TLS wrapper 的
  内层 cmux 在正常关闭时返回 `cmux.ErrListenerClosed`/`ErrServerClosed`，会被当作
  Endpoint 错误上抛；内部 HTTP listener 被外层关闭后再次 Close 返回的
  `net.ErrClosed` 也被记为 Error，而真正的 internal close 错误反而被吞掉。现由统一
  `normalizeServeError` 归一 HTTP、gRPC、net 和 cmux 的关闭 sentinel，root、普通
  subserver 和 TLS nested mux 共用；TLS close 只忽略预期错误，并聚合返回真正错误。
  既有 TLS+明文双模式集成测试不再忽略 `Endpoint.Run` 结果，明确断言取消返回 nil；
  新增表驱动 sentinel/wrapped error 与 TLS internal close 回归。生产 TLS smoke
  同时修复清单已从 Deployment 改为 StatefulSet、脚本仍等待旧 resource kind 导致
  门禁必然失败的问题。TLS 双模式普通 20 轮、endpoint race、full test/full vet
  通过；修正后的 TLS-only StatefulSet smoke 以 mTLS 完成 Put/Get 和 1.5 MiB
  request-limit 校验。三副本 KubeBrain + 独立 TiKV/PD 进一步完成 TLS 双栈冷启动
  durable state 恢复、mTLS CRUD/Status 和 leader 删除：修复前可见
  `tls internal server close err`，最终两个 TLS nested server、三个 root listener
  与 Endpoint 均只以 Info 退出，约 10ms 进入 TiKV/PD client 关闭，无 usage、panic
  或强制退出，并自动恢复 3/3 Ready。验证后共享集群恢复明文访问参数并保留 A94
  镜像。exact image
  `36ab71d84bd626349fcd5aa21b9ee137669574f8185699586b8227564ed6fb2f`。
- **Runtime A95 empty TLS configuration semantics（2026-07-17）**：对照 etcd
  `client/pkg/transport/listener.go:TLSInfo.Empty`，明文 listener 不应初始化 TLS。
  KubeBrain 的 option 层始终创建三个空 `SecurityConfig`；`Validate` 虽正确判定为
  insecure，`getServerConfig` 随后却只检查指针非 nil，再次调用 `init` 加载空
  cert/key，忽略返回错误后碰巧得到 nil TLS config。协议选择最终仍为明文，但每个
  Pod 启动固定产生两条 `open : no such file or directory` Error，污染告警并掩盖
  真正的证书故障。现 TLS getter 与 server config 传播都显式保持 Empty→nil，不再
  触发初始化；`isInsecure` 同时纳入 ServerName 与 CN/hostname allowlist，避免只配置
  身份约束时被静默视为明文，改为明确拒绝缺少 client auth/CA 的无效配置。回归固定
  client/peer server/client TLS config 均为 nil、代理 TLS 为 nil，以及两类孤立
  allowlist fail closed；focused 普通 50 轮、race 20 轮、endpoint/option/server/
  proxy race、full test 与 full vet 全通过。三副本 KubeBrain + 独立 TiKV/PD 重建后，
  三个 Pod 的空 keypair Error 均由 2 降为 0，client/peer/info 都明确
  `ONLY_INSECURE`，内部代理记录 `secure=false`；部署 3/3 Ready、zero restart，
  health 与 CRUD 正常，且无临时 TLS volume 残留。exact image
  `e0893bbd5215d124115212d395ea8bea8d5a28a65dbee5b4844d88cf008e8511`。
- **Endpoint A96 etcd-compatible health semantics（2026-07-17）**：对照
  `/root/etcd/server/etcdserver/api/etcdhttp/health.go`，etcd `/health` 会先检查
  leader，再执行受 ReqTimeout 限制的真实 Range；`serializable=true` 只允许无
  leader 场景，仍必须完成存储读。KubeBrain 原实现对任意 GET 固定返回
  `{"health":"true"}`，即使没有 leader 或 TiKV 已不可读也报 200。现普通
  `/health` 先要求本机 leader serving 或 follower proxy ready，再通过
  `brainServer.Get` 实际执行 `SyncReadRevision + backend Get`，避免只相信陈旧代理
  缓存；serializable 模式直接探测共享存储。失败返回 503、`health=false` 和
  `RAFT NO LEADER`/`RANGE ERROR` reason。`/ready` 复用同一真实线性读，不再只看
  内存状态；A96 当时新增与后端无关、只表示进程 HTTP stack 存活的 `/livez`，
  dev、plain production 与 TLS production 的 liveness/startup probe 当时迁移到
  `/livez`，
  防止 TiKV/选主故障触发无效重启，manifest 回归固定三类 probe 路径。确定性测试
  覆盖无 leader 503、serializable 200、storage error 503、livez 200，focused
  普通 50 轮、race 20 轮、server/endpoint/manifest race、full test 与 full vet
  通过。三副本 KubeBrain + 独立 TiKV/PD 上逐 Pod 直连时 leader/follower 的
  health、serializable、ready、livez 全部 200；固定直连 follower 后删除 leader，
  250ms 内观测 `health=503/serializable=200/livez=200` 和 `RAFT NO LEADER`，
  新 leader 完成 durable 初始化约 8.75 秒后 health 自动恢复 200。最终部署
  3/3 Ready、zero restart，CRUD 正常。后续 A97 源码复核确认 etcd `/livez`
  实际包含 serializable read，此命名与 `/readyz` 分项差距已在 A97 修正。exact image
  `f33a90074fb0f3f5029ebd0249a3514ba09d24c635280ccea1e7e1f4432b97c9`。
- **Endpoint A97 etcd livez/readyz check matrix（2026-07-17）**：进一步对照
  `/root/etcd/server/etcdserver/api/etcdhttp/health.go` 的
  `installLivezEndpoints`、`installReadyzEndpoints`、`newHealthHandler` 及
  `health_test.go`，确认 etcd `/livez` 并非纯进程检查，而是执行
  `serializable_read`；`/readyz` 依次暴露 `data_corruption`、
  `serializable_read`、`linearizable_read`、`non_learner`，根端点支持重复
  `exclude` 与按参数存在性启用的 `verbose`，失败时始终输出分项原因并返回 503。
  现 client/info 两个 HTTP 端口均注册根端点和全部单项端点，固定 GET-only、
  `Allow: GET`、text/plain、nosniff、`ok\n` 与详细结果格式。KubeBrain 不持有
  TiKV Raft learner 身份，也没有 member-local CORRUPT alarm，因此
  `non_learner` 与 `data_corruption` 使用平台等价的通过检查；存储完整性由 TiKV
  及 KubeBrain Hash/GC 检查负责。纯进程 HTTP 存活语义迁移到平台端点 `/ping`，
  dev、plain production 与 TLS production 的 liveness/startup probe 同步迁移，
  避免 TiKV 或选主抖动引发无效 Pod 重启；`/ready` 保留为兼容别名。确定性测试覆盖
  default/verbose/exclude、全部单项路径、405、无 leader、storage error 及
  `/ping` 隔离；full test、server/manifest race 与 full vet 全通过。三副本
  KubeBrain + 独立 TiKV/PD 上，所有根端点和单项检查均为 200；固定直连 follower
  后删除 leader，200ms 采样中 `/readyz` 连续 503，详细结果仅
  `linearizable_read` 报 `RAFT NO LEADER`，同期 `/livez` 与 `/ping` 始终 200，
  约 1.6 秒后 `/readyz` 自动恢复。最终部署 3/3 Ready、zero restart。containerd
  运行时 exact image
  `73bfdfb4eedb9b5a3a1b65ac1afaba14012c7f670431e1aaac39f5ee0d246836`。
- **Endpoint A98 health-check observability（2026-07-17）**：继续对照
  `/root/etcd/server/etcdserver/api/etcdhttp/health.go` 的 `recordMetrics` 及
  `health_test.go:checkMetrics`，A97 虽对齐 HTTP 契约，但没有输出 etcd 的检查级
  Prometheus 指标，DBaaS 无法仅凭 metrics 区分后端不可读和线性读无 leader。
  现每次实际执行的 livez/readyz 分项检查均更新
  `etcd_server_healthcheck{type,name}` gauge，并累加
  `etcd_server_healthchecks_total{type,name,status}`；root `exclude` 跳过的检查不
  产生样本，type/name/status 全部来自固定注册表，不接受用户输入，避免高基数。
  传统 `/health` 同时补齐 `etcd_server_health_success` 与
  `etcd_server_health_failures` counter。结构化 recorder 回归固定成功/失败值、
  标签、exclude 与 legacy counter；full test、server race 和 full vet 全通过。
  三副本 KubeBrain + 独立 TiKV/PD 正常态逐 Pod 抓取 `/metrics`，五个分项 gauge
  均为 1，success counter 均存在；删除 leader 并固定直连 follower 后，第一个
  `/readyz` 503 采样即观测 `linearizable_read` gauge=0、error counter=1，而
  serializable/data/non-learner 保持成功。恢复后 gauge 自动回到 1，error counter
  保留累计值（故障窗口最终为 15），部署恢复 3/3 Ready、zero restart。同步修正
  observability 文档中已过时的“丢 leader 会重启”说明，并新增换主与共享存储不可读
  的分项告警建议。containerd 运行时 exact image
  `49fedf479a7c029b2fd92509aed94d3bcafd97f31709721293de1d8afb029955`。
- **Txn/Runtime A99 uncertain marker compaction pin and in-process
  re-election（2026-07-17）**：复核 A74 的 multi-key commit-undetermined
  resolver 与 `/root/etcd/server/etcdserver/txn` 的单 apply 顺序后发现，单键
  uncertain retry 通过 `asyncFifoRetry.MinRevision()` 阻止 Compact 越过待判定
  revision，但事务级 resolver 未进入该队列。若 marker 读取持续失败，同时 physical
  Compact 清理该 revision 的 event-log marker，已提交事务随后会被误判为
  not-committed，collector 跳过 revision 且 Watch 丢失整批事件。现 backend 使用
  引用计数 revision pin 跟踪所有事务级 uncertain 判定；pin 在启动 resolver 前注册，
  committed、not-committed、shutdown 或 worker 拒绝路径均释放；
  `clampCompactRevision` 取单键 retry 与事务 pin 的最小 revision，因此 logical
  watermark、physical GC 和 event-log cleanup 都不能越过未判定事务。确定性故障测试
  让已提交 uncertain Txn 卡在 marker read，证明并发 compact 被限制在
  `revision-1`，恢复后事务以单 revision 发布、pin 释放且 compact 可继续；相关测试
  连续 20 轮通过。

  真实单 PD 删除测试又暴露 `leaderElection.Campaign` 在 renew deadline 后仍调用
  `RunOrDie` callback 中的 `klog.Fatal("leader lost")`，导致原 leader 以 255 退出，
  与 DBaaS 后端短暂故障不应重启数据面进程的目标冲突。现每轮使用
  `NewLeaderElector.Run`，丢失 leadership 时先原子撤销 leader/serving、触发 lease
  cleanup 和写 epoch fence，再在同一进程重新 campaign；初始化 election record
  失败也取消本轮并重试，不再 fatal。脚本 resource lock 连续 3 轮证明 renew 失败、
  stopped callback、存储恢复、第二次 started callback 与 context 正常退出。
  final full test、backend+leader race 和 full vet 全通过。最终镜像在三副本
  KubeBrain + 独立单 TiKV/单 PD 上删除 `kb-pd-0`，捕获 4 个 ambiguous 多键 Txn，
  Porcupine 完整历史为 `Ok`；原 leader 约 1 秒后同进程重新当选，三个 Pod
  zero restart。故障后 `TestPhysicalCompactionUnderTraffic` 通过，endpoint health
  约 32ms，证明无 pin 泄漏或 compactor 停滞。containerd 运行时 exact image
  `9e574c54fc907bfdc7c951e2356c886d4bde636b2f2a66911f3d9e35e11d0df2`。
- **Runtime A100 leadership term callback drain（2026-07-17）**：继续审计
  A99 的同进程重新 campaign，并对照 client-go `leaderelection.LeaderElector.Run`
  发现：`OnStartedLeading` 在独立 goroutine 中执行，`Run` 丢失 leadership 后只
  cancel 其 context，不等待 callback 退出。A99 外层循环因此可能在旧 term 的
  `ReloadLeases`、event-log watermark、count index 或 physical compaction 初始化
  尚未收尾时启动新 term，形成同进程跨 term 并发。现每轮 election 通过成功的
  resource-lock Create/Update 标记已获取 leadership；`OnStoppedLeading` 仍立即撤销
  serving、lease 与写 epoch，随后等待该轮 started callback 完整退出，才允许构造
  下一轮 elector。若 acquisition 在成功前被取消，则不等待不存在的 callback。
  确定性测试让首轮 callback 收到 cancel 后继续执行 250ms 清理，并连续 10 轮验证
  重获 leadership 且 callback 最大并发恒为 1；full test、leader race 与 full vet
  全通过。

  最终镜像在三副本 KubeBrain + 独立单 TiKV/单 PD 上删除 `kb-pd-0`，捕获 2 个
  ambiguous 多键 Txn，Porcupine 完整历史通过；原 leader 记录 leadership lost 后
  约 1 秒在同一进程重新当选，三个 Pod 均 Ready、zero restart。故障后
  `TestPhysicalCompactionUnderTraffic` 与 endpoint health 通过，health proposal
  约 29ms。containerd 运行时 exact image
  `7a446d7d10bef67974164ede87a5573ff0ea9ff065f1bc189faa752daedc87f7`。
- **Admission A101 streaming request payload limit（2026-07-17）**：复核
  `--max-request-bytes` 的全入口契约时发现，unary interceptor 会按
  `proto.Size(request)` 严格拒绝逻辑 payload 超限，但 stream interceptor 只依赖
  gRPC `MaxRecvMsgSize(max+512KiB)`。后者与 `/root/etcd/server/etcdserver/api/v3rpc/grpc.go`
  一样为 protobuf/transport 开销保留窗口，不能替代 KubeBrain 已对 unary 承诺的实例
  payload 限额；恶意 Watch create 可用超长 key/range 在同一连接反复分配并绕过
  production 1.5MiB 上限。现统一 stamped stream 在底层成功解码每条入站 protobuf 后
  执行相同的逻辑大小检查，Watch（以及未来任何新增 client/peer stream 消息）超限均
  返回 `InvalidArgument: etcdserver: request is too large`，不会进入业务 handler；
  固定只有 lease ID 的 KeepAlive 消息也经过同一路径。Watch 接收日志同时把
  `io.EOF`、客户端取消、payload 超限与 QPS admission 拒绝归为正常 stream 关闭，
  不再允许合法关闭或恶意超限请求污染 Error 告警；内部/transport 异常仍保留 Error。

  bufconn 黑盒固定 64-byte 上限下 128-byte key 能通过 transport decoder、但被逻辑层
  拒绝；focused 普通 20 轮、race 5 轮、full test、完整 etcd server race 与 full vet
  全通过。三副本 KubeBrain + 独立 TiKV/PD 的 production 1.5MiB 配置上，1.75MiB
  Watch create 连续 10 轮均返回标准错误，随后正常 Kubernetes Watch+Lease 流通过，
  且两种关闭均无 Error/Fatal 日志；endpoint health 约 37ms，三个 Pod 均 Ready、
  zero restart。containerd 运行时
  exact image
  `7f8f99e8be078a443fc2f669a0355086a6680a0b88828ea0872d6ede5c70993a`。
- **RangeStream A102 configurable chunk target and terminal metadata
  （2026-07-17）**：对照
  `/root/etcd/server/etcdserver/v3_server.go:rangeStream` 与 client/v3
  `RangeStreamResponse` 契约发现两处差距。KubeBrain scanner 固定按约 1.5MiB 内部
  吞吐批次输出，未随 `--max-request-bytes` 较小配置二次切分；`Limit>0` 更直接复用
  unary Range 后整包发送，大 bounded list 会失去流式内存/wire 边界。其次，etcd
  只在最后一个 chunk 设置 `Header/More/Count`，KubeBrain 却在每个 scanner chunk
  重复 Header，已有测试还错误固化了该行为。

  现保留 scanner 内部批次以免影响 count-index rebuild 和 TiKV 扫描吞吐，在公开 RPC
  边界按实际 protobuf wire shape 贪心二次切分；unlimited 与 bounded 路径统一受配置
  目标约束，单个不可拆 KV 超限时单独发送。切分器预计算每个 KV 的 protobuf 字段长度
  前缀和，候选大小 O(1)、整批 O(n)，避免反复扫描大 value。中间 chunk 仅含互斥 KVs，
  最后一块才携带 pinned revision、More 与总 Count。确定性回归固定 256-byte 目标、
  unlimited/bounded 内容完整有序、每块真实 `proto.Size` 不越界及 terminal-only
  metadata；focused 普通 50 轮、race 10 轮、full test、完整 server race 与 full vet
  全通过。

  三副本 KubeBrain + 独立 TiKV/PD 上写入 8 个约 320KiB value，形成约 2.5MiB 结果集；
  unlimited 与 `Limit=8` 两条 client/v3 GetStream 路径连续 5 轮均完整返回 8 键、产生
  多块且每块不超过 production 1.5MiB。对本机 `/root/etcd/bin/etcd` 的
  Unlimited/Limited/CountOnly/KeysOnly/显式升序差分连续 5 轮通过。最终三个 Pod
  Ready、zero restart，无 RangeStream failure/Panic/Fatal，endpoint health 约 60ms。
  containerd 运行时 exact image
  `ca22cca67771b25e78f4835119923009d8bbcdd19530a826d2526fb5c9ed7cb3`。
- **RangeStream A103 bounded scan without unary materialization
  （2026-07-17）**：A102 对齐 wire chunk 后继续审计发现，`Limit>0` 分支仍先调用
  unary `Range/List` 物化全部返回 KVs，再把结果切成多个消息；网络块虽受限，服务端
  峰值内存仍随 Limit/对象大小增长，违背 etcd 3.7 RangeStream 为大 LIST 限制内存的
  核心目的。对照 `/root/etcd/server/etcdserver/v3_server.go:rangeStream` 到达 limit
  后对剩余范围计数的方式，现 bounded 请求也直接进入 pinned partition scanner：
  只发送前 Limit 个有序 KV，后续 scanner chunk 立即丢弃 value 引用并仅累计键数，
  最终 header-only chunk 返回同一 revision 的精确总 Count，以及
  `More=(total>sent)`。因此内存始终受 scanner worker/buffer/chunk 上限约束，不依赖
  count-index 是否 ready，也不产生第二个 revision 窗口。`CountOnly` 因无 KV payload
  继续走专用精确 Count。完成条件同时改为必须观察到 scanner terminal marker；若后端
  在已发送部分 KVs 后提前关闭 channel，则返回 Unavailable 使 client/v3 丢弃部分结果，
  不再因“曾发送过数据”而误报成功。

  确定性回归固定 12 键、Limit=5，验证只返回 5 键、Count=12、More=true、terminal
  metadata 唯一，且 instrumentation 证明 unary `List` 调用次数为 0；提前关闭回归
  固定部分数据后无 terminal 必须返回 Unavailable。RangeStream focused 普通 50 轮、
  race 10 轮、full test、完整 server race 与 full vet 全通过。
  三副本 KubeBrain + 独立 TiKV/PD 上用 9 个约 320KiB value 验证约 2.8MiB 结果集：
  unlimited 为 9/Count=9/More=false，Limit=8 为 8/Count=9/More=true，两条路径连续
  5 轮通过且每个 wire chunk 不超过 production 1.5MiB。对本机 reference etcd 的
  五类 RangeStream 差分再连续 5 轮通过。最终三个 Pod Ready、zero restart，无
  RangeStream failure/Panic/Fatal，endpoint health 约 47ms。containerd 运行时
  exact image
  `4db3cea1825122acf0924f597620373c6b9c45c74ffb4c9938638f5c4754d8f1`。
- **Cluster A104 linearizable MemberList barrier error contract
  （2026-07-17）**：对照 `/root/etcd/server/etcdserver/api/v3rpc/member.go` 的
  `togRPCError` 边界审计发现，KubeBrain 的 `MemberList(linearizable=true)` 直接返回
  `SyncReadRevision` 普通错误，gRPC 会把 leader/read-barrier 瞬态故障暴露为
  `Unknown`，客户端无法按可重试的集群暂不可用处理。现仅在 Cluster RPC 边界把无
  status 的 barrier 错误整形为 `Unavailable`；已有 gRPC status 和
  `Canceled`/`DeadlineExceeded` 原样保留。回归测试连续 20 轮固定普通错误、
  context 错误及已有 `ResourceExhausted` 的映射。

  `go test ./...`、`go vet ./...` 和完整 `pkg/server/etcd` race（235.868s）通过。
  A104 镜像滚动到三副本 KubeBrain + 独立 TiKV/PD 后，线性化 MemberList、
  endpoint status 及线性化 Put/Get/Delete 均成功；三个 Pod 全部 Ready、zero
  restart，运行时 exact image
  `e7776e28c0284f75b1ca150a9a3fcaaaad9e40a115308cb5a62db8ea7bdbc782`。
  当前 dev Deployment 未注入静态 `initial-cluster`，因此黑盒 MemberList 按文档
  回退为当前副本加 leader；production StatefulSet 的固定三成员配置继续由解析与
  MemberList 测试覆盖。
- **RPC A105 read-barrier failure classification（2026-07-17）**：A104 只修复
  MemberList 后系统枚举发现，Range、RangeStream、Watch create/cancel、
  Status、Hash/HashKV、Compact、范围 DeleteRange 及 generic txn fallback 的
  `SyncReadRevision` 失败仍可能泄漏为 gRPC `Unknown`。KubeBrain revision syncer
  的全部失败来源（无 leader、HTTP 拒绝/超时、非 200、畸形或零 revision）都表示
  follower 无法建立线性化 fence，语义上属于暂不可用。现抽取共享
  `readBarrierStatusErr` 并覆盖全部 10 个调用点：普通错误映射为 `Unavailable`，
  已有 gRPC status 与 `Canceled`/`DeadlineExceeded` 原样保留。Watch 在 barrier
  失败时不会发送 created 响应，避免客户端接受没有 revision fence 的 stream。

  跨 RPC 表驱动回归覆盖 Range、Compact、DeleteRange、Status、Hash/HashKV，
  独立覆盖 Watch 及 status/context 保真；focused 普通 30 轮、focused race 10 轮、
  `go test ./...`、`go vet ./...` 和完整 server race（237.184s）通过。A105 镜像
  滚动到三副本 KubeBrain + 独立 TiKV/PD 后，逐 Pod 判定 1 leader + 2 follower，
  三个端点的 Status、线性化 Range、MemberList 均成功，三个独立 Watch 都收到同一
  PUT。最终三个 Pod Ready、zero restart，无 error/Panic/Fatal，运行时 exact image
  `4407c535d1103348e4a4924dbb87c225b56eeb983a047a4f6a89d84e3fbf5d26`。
- **Maintenance A106 cached RaftTerm response path（2026-07-17）**：继续对照
  `/root/etcd/server/etcdserver/api/v3rpc/maintenance.go:Status` 发现，reference
  etcd 直接从内存 Raft 状态读取 term，Status 不会因额外存储查询失败；KubeBrain
  虽已在 A44/A47 提供共享正 term，却在每次 Status 中重新读取 TiKV-backed election
  record。更广泛地，unary interceptor 在成功 handler 后补 ResponseHeader 时，若
  本地 term cache 尚为零且首次共享记录读取失败，会丢弃成功响应并泄漏 gRPC
  `Unknown`。

  现 Status 与所有 unary header stamping 统一复用 `responseRaftTerm`：优先读取
  election renew/get 已维护的单调 atomic cache，仅启动期 cache 为零时查询共享记录；
  该首次查询失败按协调状态暂不可用整形为 `Unavailable`，已有 status/context 保持
  不变。回归测试固定 cache 命中绝不访问 TiKV lock、Status 返回缓存 term，以及首次
  查询失败不再为 Unknown。focused 普通 30 轮、focused race 10 轮、
  `go test ./...`、`go vet ./...` 与完整 server race（230.531s）通过。A106 镜像
  滚动到三副本 KubeBrain + 独立 TiKV/PD 后，逐 Pod 各 20 次 endpoint status：
  1 leader + 2 follower 共 60 次全部报告相同 leader，顶层 `RaftTerm` 与 header
  `raft_term` 均稳定为 231。最终三个 Pod Ready、zero restart，无
  error/Panic/Fatal，运行时 exact image
  `b5ba98f3a3a51660043d1e5aa36ae5a4a04d75e771661986eb08b53db940d13a`。
- **RangeStream A107 nested response identity header（2026-07-17）**：对照
  `/root/etcd/server/etcdserver/api/v3rpc/key.go:headerFillingRangeStream` 发现，
  `RangeStreamResponse` 不在顶层暴露 `GetHeader()`，真实 header 位于
  `range_response.header`。KubeBrain 的统一 stream interceptor 只识别直接 header，
  因此 A102/A103 虽正确返回 pinned revision、Count 与 More，却把 ClusterId、
  MemberId、RaftTerm 留为零；这会破坏依赖 cluster identity 的 client interceptor，
  且与其他 etcd RPC 不一致。

  `stampHeader` 现显式识别 RangeStream 嵌套响应，不为 nil payload/header 合成结构；
  其余直接-header RPC 与 Defragment 的 nil-header 契约不变。helper 回归固定嵌套
  三字段，bufconn 真实 gRPC 回归固定至少 data+terminal 两块、仅 terminal 一块带
  header，且 revision/cluster/member/term 全非零。focused 普通 50 轮、focused
  race 10 轮、`go test ./...`、`go vet ./...` 与完整 server race（235.085s）
  通过。A107 三副本 + 独立 TiKV/PD 上使用 `/root/etcd/bin/etcdctl get --stream
  -w json` 与临时 reference etcd 差分：两端均返回完整非零四字段、4 KVs、Count=4。
  最终三个 Pod Ready、zero restart，无 error/Panic/Fatal，运行时 exact image
  `517e5a8f66891a5da531ab15b6fb67ebf7e8c64f1a731d72b44ccc6034656fa8`。
- **Txn A108 unconditional Success with unreachable Failure（2026-07-17）**：
  对照 reference etcd 的 Txn apply 选择规则发现，合法请求在 `Compare` 为空时总是
  选择 `Success`；即使 `Failure` 非空也不会执行。KubeBrain 原先只把
  `Compare`、`Failure` 同时为空识别为 simple Txn，因此这类请求会落入
  `unsupported transaction`，并向官方 client/v3 泄漏 gRPC `Unknown`。

  现无 Compare 的请求统一进入 generic atomic Txn executor；不可达的 Failure 仍由
  入口完成结构校验、操作数限制和鉴权，但不再影响分支选择。顶层与嵌套回归分别固定
  空 Success、带写 Success 及不可达 Failure 不产生读响应或写入。focused 普通
  30 轮、focused race 10 轮、`go test ./...`、`go vet ./...`、compat module
  `go vet ./...` 与完整 server race（230.819s）通过。A108 镜像滚动到三副本
  KubeBrain + 独立 TiKV/PD 后，官方 client/v3 对临时 reference etcd 与
  `127.0.0.1:4379` 连续 10 轮差分，Succeeded、响应数、相对 revision、Success
  写值及 Failure key 不存在全部一致。最终三个 Pod Ready、zero restart，外部端点
  status/health 正常，运行时 exact image
  `6335acb1c5826769d61888ac08d5ccef0d79e076c58ef8ac1495c32c897457d1`。
- **Lease/Range A109 authoritative unleased version metadata（2026-07-17）**：
  对照 etcd 每个 MVCC `KeyValue.Lease` 属于该版本的语义发现，KubeBrain 的 v1
  value envelope 虽明确表示该版本无 lease，响应转换器却把 `Lease=0` 当作元数据
  缺失，并回退到 leader-only 的当前 key-to-lease 索引。一个 key 从无租约版本更新
  为有租约版本后，读取旧 revision 因而会错误返回当前 lease；同一请求打到 follower
  时还可能因本地索引为空而得到不同结果。

  现 v1/v2 envelope 都作为权威 per-version lease 元数据：v1 固定为无租约，v2
  返回其持久化 lease ID；仅升级前真正没有 envelope 的 legacy raw value 继续回退
  当前索引。回归测试固定历史无租约版本不会继承当前 lease，且当前有租约版本不变。
  focused 普通 30 轮、`go test ./...`、根模块与 compat module `go vet ./...`、
  完整 server race（226.896s）通过。官方 client/v3 对临时 reference etcd 与
  TiKV-backed KubeBrain 连续 10 轮差分，依次验证无租约、有租约、再次无租约三个
  版本的相对 revision、历史 lease 与当前值完全一致；再经三个 Pod 独立 port-forward
  执行 serializable historical Range，三个副本均返回旧值且 lease 为零。最终三个
  Pod Ready、zero restart，外部 endpoint health 正常，运行时 exact image
  `26409541219fe09070769ed3b314d6961cce0892a26e7ad911a6f78b019ca092`。
- **Range A110 option-combination differential expansion（2026-07-17）**：
  继续对照 `/root/etcd/server/etcdserver/txn/range.go` 的 filter、sort、limit 与
  response assembly 顺序，扩展官方 client/v3 双端差分矩阵。新增 filtered
  CountOnly（无 KVs、Count 保持过滤前范围总数）、revision filter + limit 的
  More/Count、point CountOnly、point KeysOnly，以及负 revision 读取当前快照；
  所有结果结构化比较 header 相对 revision、KV metadata/value、Count 和 More。

  这轮审计也排除了三个疑点：负 revision 在双端都按当前快照处理；staged Txn
  每次 Range 都克隆 base/staged KV，KeysOnly 不会污染后续事务视图；Watch
  fragment 的阈值、字段复制与末片标志已和 upstream 一致。扩展后的 Range
  differential 在临时 reference etcd 与在线三副本 TiKV-backed KubeBrain 间连续
  10 轮通过，race 下再连续 3 轮通过；`go test ./...`、根模块与 compat module
  `go vet ./...` 及完整 server race（202.756s）通过。A110 仅增加兼容性测试，
  不改变服务二进制；验证时三个 Pod Ready、zero restart、endpoint health 正常，
  运行时 exact image 仍为
  `26409541219fe09070769ed3b314d6961cce0892a26e7ad911a6f78b019ca092`。
- **Maintenance A111 Hash snapshot header fencing（2026-07-17）**：
  对照 `/root/etcd/server/storage/mvcc/hash.go:HashByRev` 与
  `/root/etcd/server/etcdserver/api/v3rpc/maintenance.go:HashKV`，发现
  KubeBrain backend 只返回 hashed revision，handler 在 hash 完成后重新读取 current
  revision 填 Header。并发写可使 Header 指向摘要并未覆盖的更新，修复前在线三副本
  实测 `HashRevision=467751513260556468`、`Header.Revision=467751513260556477`。

  现 backend 在 `logicalWriteMu` 内同时捕获 hashed revision 与 snapshot current
  revision，`Hash`/`HashKV` 使用后者填 Header，保留负数或历史请求的
  `HashRevision` 语义。确定性单测在 hash 返回前注入一次写，证明 Header 固定在旧
  snapshot；官方 client/v3 在持续写入下反复执行 `HashKV(0)`，要求
  `Header.Revision == HashRevision`。该测试在 `/root/etcd` reference 连续 3 轮通过，
  新 TiKV-backed KubeBrain 单轮、连续 3 轮及 race 均通过；`go test ./...`、根模块
  与 compat module `go vet ./...` 及完整 server race（222.918s）通过。部署
  `kubebrain:a111-hash-snapshot` 后三个 Pod Ready、zero restart、endpoint health
  正常，运行时 exact image
  `7a9baf3394131856e5620225c0172c3cefcb4f11e6a38460f6c597f7aa341dd1`。
- **Maintenance A112 logical-compaction-aware HashKV（2026-07-18）**：
  继续对照 `/root/etcd/server/storage/mvcc/hash.go:unsafeHashByRev` 与
  `hashByRev`，发现 A111 后 `CompactRevision` 仍在 hash 锁释放后另行读取，且
  KubeBrain 摘要所有尚未被异步物理 GC 删除的 MVCC 行。logical Compact 已成功但
  physical GC 尚未追上时，响应会同时携带新 watermark 和旧物理摘要；修复前在线
  A111 镜像稳定复现 Compact 后 Hash 仍为 `0xd06ee218`，未排除已计划删除的版本。

  现 backend `HashKVResult` 从一个 write-fenced snapshot 原子返回 Hash、
  HashRevision、CurrentRevision 与 CompactRevision；compact watermark CAS 与 hash
  通过同一 `logicalWriteMu` 串行。hash 在 watermark 以下按 user key 仅保留最新
  live version、排除 tombstone，在 watermark 以上保留目标 revision 可见的版本，
  与 physical compactor 的保留规则一致，因此异步 GC 前后摘要稳定。historical/
  future revision 也在同一锁内对 current/compact snapshot 复核，避免 handler 外层
  校验后 watermark 又推进。确定性测试覆盖 live 多版本与已删除 key、hash 后立即
  推进 watermark 的字段撕裂及校验后并发 compact，并连续 10 轮及 race 通过。官方
  client/v3 场景在 `/root/etcd` reference 连续 3 轮通过；真实
  TiKV-backed KubeBrain 连续 3 轮、race 1 轮通过，A111 并发 Header 回归再连续
  3 轮通过。`go test ./...`、根模块与 compat module `go vet ./...`、compact/hash
  聚焦回归 3 轮及最终完整 server race（232.211s）通过。部署
  `kubebrain:a112-logical-hash` 后三个 Pod Ready、zero restart、endpoint health
  正常，运行时 exact image
  `27820dd7a512621a16182ea03afb63f716f4d4ecc6206fcd18ef729291a5a005`。
- **Txn A113 staged CountOnly ignores Limit（2026-07-18）**：继续对照
  `/root/etcd/server/etcdserver/txn/range.go` 的 `executeRange` 与
  `asembleRangeResponse` 发现，reference etcd 的 MVCC CountOnly 只返回总数而不返回
  KVs，因此即使请求同时携带 Limit，也不会截断 KVs 或报告 `More`。KubeBrain 普通
  Range 已满足该契约，但 staged Txn Range 原先先应用 Limit、设置 `More=true`，再
  清空 CountOnly 的 KVs。修复前官方 client/v3 双端差分精确复现：两端 Count=2、
  KVs=0，仅 KubeBrain `More=true`。

  staged executor 现于 filter 后直接完成 CountOnly 响应，跳过无可观察意义的排序、
  Limit 和 KeysOnly；普通 staged Range 的 Limit/More 行为保持不变。单元回归将
  CountOnly 与 Limit=1 组合并固定 `More=false`，官方 client/v3 差分也结构化比较
  `CountMore`。聚焦回归连续 30 轮、`go test ./...`、根模块与 compat module
  `go vet ./...`、完整 server race（212.484s）通过。部署到三副本 + 独立 TiKV/PD
  后，临时 `/root/etcd` reference 与在线 KubeBrain 串行差分连续 10 轮、race 3 轮
  通过；三个 Pod 均 Ready、zero restart，endpoint health 正常，运行时 exact image
  `9aad3d31431d75d33ada93cefde660d2a06b3cd644d818b9a4fbf074733a158f`。
- **Range A114 boundary-combination differential expansion（2026-07-18）**：
  继续对照 `/root/etcd/server/etcdserver/txn/range.go` 的 `rangeLimit`、
  `filterRangeResults`、`sortRangeResults` 与 `asembleRangeResponse`，扩展官方
  client/v3 双端差分矩阵。新增 MaxModRevision、MaxCreateRevision、Create/Mod/
  Version 排序、KeysOnly + Value 降序 + Limit、缺失 point CountOnly、反向空区间及
  负 Limit；每项都比较 header 相对 revision、完整 KV metadata、Count 与 More。

  实际 reference 探测确认负 Limit 在 unary Range 与 staged Txn Range 中都按不限量
  处理；其余新增组合也未发现 A113 运行代码与 reference 的差异，因此本里程碑只把
  这些边界固化为发布回归门槛，不引入无证据的服务实现改动。扩展矩阵在临时
  `/root/etcd` reference 与在线三副本 TiKV-backed KubeBrain 间连续 10 轮通过，
  race 下连续 3 轮通过；Range 聚焦测试、`go test ./...`、根模块与 compat module
  `go vet ./...` 及完整 server race（200.694s）通过。服务二进制未变化，三个 Pod
  继续 Ready、zero restart、endpoint health 正常，运行时 exact image 仍为
  `9aad3d31431d75d33ada93cefde660d2a06b3cd644d818b9a4fbf074733a158f`。
- **Txn A115 unknown Compare enum fallthrough（2026-07-18）**：对照
  `/root/etcd/server/etcdserver/api/v3rpc/key.go:checkTxnRequest` 与
  `/root/etcd/server/etcdserver/txn/txn.go:compareKV` 发现，reference 只校验
  Compare key，不拒绝 protobuf 未知 Result/Target 枚举。其 apply 规则是：未知
  Result 跳过已知分支并返回 true；未知 Target 保持比较序值为零，再按 Result 判定。
  KubeBrain 原先在 RPC 入口主动返回 InvalidArgument。raw gRPC 修复前双端探测中，
  reference 对未知 Result、未知 Target + EQUAL 都返回 OK、选择 Success，KubeBrain
  分别返回 `invalid compare result` / `invalid compare target`。

  现移除额外 enum validation，并将 point/range compare 共用的结果判定对齐为上述
  fallthrough；空 Compare key 仍按双方契约返回 InvalidArgument。单元测试覆盖未知
  Result、未知 Target + EQUAL/NOT_EQUAL 的分支选择，raw protobuf 官方 API 差分同时
  比较 gRPC 状态、Succeeded 与最终写值。聚焦回归连续 20 轮、`go test ./...`、
  根模块与 compat module `go vet ./...` 及完整 server race（206.164s）通过。
  临时 `/root/etcd` reference 与在线三副本 TiKV-backed KubeBrain 修复后差分连续
  10 轮、race 3 轮通过。部署 `kubebrain:a115-compare-enums` 后三个 Pod Ready、
  zero restart、endpoint health 正常，运行时 exact image
  `91343632e7ebe87c1733bc03156c7a8183ab8607ee8f2a80583da170fbf8fd6d`。
- **Watch A116 unknown/duplicate filter differential（2026-07-18）**：对照
  `/root/etcd/server/etcdserver/api/v3rpc/watch.go:FiltersFromRequest` 与
  `/root/etcd/server/storage/mvcc/watchable_store.go`，补齐 raw protobuf WatchCreate
  filter 边界。reference 对未知 FilterType 不返回错误，而是忽略该项；重复 NOPUT
  等价于一个 NOPUT，不会取消 watch 或重复事件。KubeBrain 的实时与历史过滤实现已
  满足该规则，本轮将其固化为官方 API 双端差分：未知 filter 的历史 watch 收到 PUT、
  DELETE，重复 NOPUT 只收到 DELETE，并继续比较既有 create/cancel/progress/fragment
  控制契约。

  新矩阵在临时 `/root/etcd` reference 与在线三副本 TiKV-backed KubeBrain 间连续
  10 轮通过，race 下连续 3 轮通过；`go test ./...`、根模块与 compat module
  `go vet ./...` 及完整 server race（202.077s）通过。本轮只增加兼容性测试，不改变
  服务二进制；三个 Pod 继续 Ready、zero restart、endpoint health 正常，运行时
  exact image 仍为
  `91343632e7ebe87c1733bc03156c7a8183ab8607ee8f2a80583da170fbf8fd6d`。
- **Txn A117 absent VALUE compare always fails（2026-07-18）**：继续对照
  `/root/etcd/server/etcdserver/txn/txn.go:applyCompare` 发现，VALUE compare 对
  不存在的 point/range 必须在 Result 判定前无条件失败，因为 protobuf 无法区分
  “不存在的 value”与空字节。KubeBrain range compare 已有该特判，point compare
  却把 nil 当空字节交给通用比较器；因此不存在 key 的 `VALUE == ""`、
  `VALUE != "value"` 及未知 Result 都错误选择 Success。修复前 raw gRPC 双端差分
  精确显示 reference 三项均 Succeeded=false、写入 Failure 值，A115 在线镜像三项
  均 Succeeded=true、写入 Success 值。

  现 point VALUE compare 在 kv=nil 时直接 false，VERSION/CREATE/MOD/LEASE 对
  absent key 继续按 upstream 使用零值，A115 的未知 Target/Result fallthrough 规则
  也保持不变。单元回归覆盖上述三种 VALUE Result 并连续 30 轮通过；扩展后的 raw
  protobuf 差分同时比较状态、Succeeded 与最终写值。`go test ./...`、根模块与
  compat module `go vet ./...` 及完整 server race（210.226s）通过。修复后临时
  `/root/etcd` reference 与在线三副本 TiKV-backed KubeBrain 差分连续 10 轮、
  race 3 轮通过。部署 `kubebrain:a117-absent-value-compare` 后三个 Pod Ready、
  zero restart、endpoint health 正常，运行时 exact image
  `279d2608ee23f986d3f122b7ca1c7fe1264ca1f990246ee98eaac9c94dd09d21`。
- **Txn A118 Compare truth-table differential matrix（2026-07-18）**：将
  `/root/etcd/server/etcdserver/txn/txn.go:applyCompare` 的 Compare 规则固化为
  24 项 raw protobuf 官方 API 双端差分。矩阵覆盖 VALUE/VERSION/CREATE/MOD/LEASE
  五类 target、EQUAL/NOT_EQUAL/LESS/GREATER 四类 result，以及 present point、
  absent point、empty range 和多 key range；revision 与 lease 断言使用各端点自身
  seed 响应中的精确值，避免把数据库全局状态差异误报为语义差异。多 key 用例同时
  验证 range Compare 要求范围内每个 KV 都满足，empty range 验证数值 target 的
  vacuous true 与 VALUE 的强制 false。

  临时 `/root/etcd` 3.8.0-alpha.0 reference 与在线三副本 TiKV-backed KubeBrain
  首轮及连续 10 轮通过，race 连续 3 轮通过，未发现 A117 之后的新实现差异。本轮
  同时通过 `go test ./...`、根模块与 compat module `go vet ./...` 及完整 server
  race（203.166s）。本轮只增加兼容性测试，不改变服务二进制；生产集群继续运行
  `kubebrain:a117-absent-value-compare`，exact image 为
  `279d2608ee23f986d3f122b7ca1c7fe1264ca1f990246ee98eaac9c94dd09d21`。
  三个 Pod 均 Ready、zero restart；分别经 Pod port-forward 验证 leader 与两个
  follower 的 Status 和写转发均正常，探针 key 已清理。
- **Txn A119 branch validation error precedence（2026-07-18）**：对照
  `/root/etcd/server/etcdserver/api/v3rpc/key.go:checkTxnRequest` 与
  `kvServer.Txn` 发现，reference 先递归校验 Success、Failure 两个分支中的全部
  RequestOp，再分别执行 put/delete interval 重叠检查。KubeBrain 原先在校验
  Failure 前先检查 Success interval，因此“Success 重复 Put 同一 key + Failure
  Put 空 key”的 raw 请求虽然双方都返回 InvalidArgument，reference message 是
  `etcdserver: key is not provided`，KubeBrain 却提前返回
  `etcdserver: duplicate key given in txn request`。

  现将两个分支的 RequestOp 校验保持在全部 interval 检查之前，duplicate/overlap
  算法和事务执行路径不变。单元回归连续 30 轮通过；raw protobuf 双端差分同时比较
  gRPC code 与完整 message，修复后在临时 `/root/etcd` 3.8.0-alpha.0 reference
  与在线三副本 TiKV-backed KubeBrain 间连续 10 轮、race 3 轮通过。`go test
  ./...`、根模块与 compat module `go vet ./...` 及完整 server race（210.254s）
  通过。部署 `kubebrain:a119-txn-validation-order` 后三个 Pod Ready、zero
  restart；逐 Pod Status 与 leader/follower 写转发正常，运行时 exact image
  `bce1813a12cf11814c08dcda3815c71e85e7733208d80b290aa94ee6ff5c1cba`。
- **Txn A120 from-key interval validation parity（2026-07-18）**：对照
  `/root/etcd/server/etcdserver/api/v3rpc/key.go:checkIntervals` 与
  `/root/etcd/pkg/adt/interval_tree.go:StringAffineComparable` 发现，reference 的
  duplicate-key 校验只将空字符串视为 affine 正无穷；标准 from-key RangeEnd
  `{0}` 仍按原始字节区间参与校验。因此同一 Txn 中 DeleteRange(from-key) 与范围内
  Put 无论先后都被 reference 接受，再由事务执行顺序决定最终 key 是否存在。
  KubeBrain 原先在自定义 interval 中把 `{0}` 特判为 open-ended，提前以
  `etcdserver: duplicate key given in txn request` 拒绝这两类请求。

  现移除校验层的额外 `{0}` open-ended 特判；实际 DeleteRange 执行层仍保持 etcd
  from-key 语义。端到端单元测试覆盖 Delete→Put 最终保留 key、Put→Delete 最终删除
  key，并连续 30 轮通过。raw protobuf 双端矩阵还覆盖范围前 Put、空 range 和反向
  range；修复前仅两项 from-key overlap 不同，修复后在临时 `/root/etcd`
  3.8.0-alpha.0 reference 与在线三副本 TiKV-backed KubeBrain 间连续 10 轮、
  race 3 轮通过。`go test ./...`、根模块与 compat module `go vet ./...` 及完整
  server race（210.599s）通过。部署 `kubebrain:a120-from-key-interval` 后三个
  Pod Ready、zero restart，逐 Pod Status 与 leader/follower 写转发正常，运行时
  exact image
  `f29788b5c40c2dadb048650036138196b8be0a19d219e1980d8bc8517a29a678`。
- **KV/Txn A121 operation validation and revision-filter Count matrix
  （2026-07-18）**：继续对照
  `/root/etcd/server/etcdserver/api/v3rpc/key.go:checkRangeRequest`、
  `checkPutRequest`、`checkDeleteRequest`，补齐 Txn 内嵌 RequestOp 的 raw protobuf
  多错误优先级差分。八项矩阵覆盖空 key 优先于 Put ignore-value、ignore-value
  优先于 ignore-lease、单独 ignore-lease、空 Range key 优先于未知 sort enum、
  未知 SortOrder/SortTarget，以及空 DeleteRange key 和空 RequestOp；双方 gRPC
  code 与完整 message 一致。

  同时新增普通 Range、CountOnly Range 和“先 Put、后 Range”的 staged Txn revision
  filter 双端矩阵，固化 upstream 的容易误解契约：`Count` 是应用 revision filter
  前请求 range 的总 key 数，`Kvs` 才是过滤后的集合，CountOnly 不返回 KVs；staged
  Put 使用事务 revision 参与 MinModRevision 过滤。本轮未发现新实现差异。两组矩阵
  在临时 `/root/etcd` 3.8.0-alpha.0 reference 与在线三副本 TiKV-backed
  KubeBrain 间连续 10 轮、race 3 轮通过；`go test ./...`、根模块与 compat module
  `go vet ./...` 及强制 `-count=1` 完整 server race（210.312s）通过。本轮只增加
  兼容性测试，不改变服务二进制；在线三副本继续运行
  `kubebrain:a120-from-key-interval`，3/3 Ready、zero restart、endpoint health
  正常，exact image 仍为
  `f29788b5c40c2dadb048650036138196b8be0a19d219e1980d8bc8517a29a678`。
- **Txn/Lease A122 ignore options and attachment transfer matrix
  （2026-07-18）**：对照
  `/root/etcd/server/etcdserver/txn/put.go:checkAndGetPrevKV`、`put` 与
  `/root/etcd/server/etcdserver/txn/txn.go:checkTxn`，补齐通用 Txn 中 ignore option
  与 lease attachment 生命周期的官方 client/v3 双端差分。场景先将 key 绑定 lease
  A，再在 Txn 中以 IgnoreValue 保留旧 value 并切换到 lease B，同时读取 staged
  Range 和 Put PrevKV；随后以 IgnoreLease 更新 value 并保持 lease B。撤销 A 后 key
  必须仍存在，撤销 B 后 key 必须消失。

  矩阵还验证 `checkTxn` 只检查 Compare 实际选中的路径：未选中的 Failure 携带不存在
  lease 不影响 Success 提交；选中同一 Failure 时双方均返回 gRPC NotFound、
  `etcdserver: requested lease not found`，且无 key 落盘。本轮未发现新实现差异。
  临时 `/root/etcd` 3.8.0-alpha.0 reference 与在线三副本 TiKV-backed KubeBrain
  首轮及连续 10 轮、race 3 轮通过；`go test ./...`、根模块与 compat module
  `go vet ./...` 及强制 `-count=1` 完整 server race（203.779s）通过。本轮只增加
  兼容性测试，不改变服务二进制；在线三副本继续运行
  `kubebrain:a120-from-key-interval`，3/3 Ready、zero restart、endpoint health
  正常，exact image 仍为
  `f29788b5c40c2dadb048650036138196b8be0a19d219e1980d8bc8517a29a678`。
- **KV/Txn A123 from-key execution order and response-state differential
  （2026-07-18）**：在 A120 已对齐 from-key 区间校验后，进一步补齐 raw protobuf
  双端执行结果差分。测试分别覆盖“先 Put d、后从 b 删除到键空间末尾”和“先删除、
  后 Put d”，逐项比较 DeleteRange `Deleted`、有序 `PrevKvs`、Txn 内 Range、事务后
  最终 Range，以及新 key 的 CreateRevision/ModRevision 是否等于事务 revision。
  前一种顺序删除 b/c/d，后一种顺序仅删除 b/c 并保留事务中新建的 d；双方响应和最终
  状态一致。

  from-key 的 `{0}` RangeEnd 会影响起始 key 之后的整个键空间。为避免兼容测试污染
  共享集群，本轮同时将相关 interval 测试和执行测试迁到 64 字节 `0xff` 开头的近最大
  专用键空间，并要求重复与 race 验证顺序执行。revision 断言只比较同一事务各响应的
  关联关系，不依赖共享在线集群可能被其他写入推进的全局 revision 增量。临时
  `/root/etcd` 3.8.0-alpha.0 reference 与在线三副本 TiKV-backed KubeBrain 连续
  10 轮、race 3 轮通过；多成员 HashKV/官方 client Sync 经 kind Pod 路由连续 3 轮
  通过，完整 compat suite 用时 67.129s。`go test ./...`、根模块与 compat module
  `go vet ./...` 及强制 `-count=1` 完整 server race（201.524s）通过。本轮只增加
  兼容性测试，不改变服务二进制；在线三副本继续运行
  `kubebrain:a120-from-key-interval`，3/3 Ready、zero restart，`/health` 与
  `/readyz` 正常，exact image 仍为
  `f29788b5c40c2dadb048650036138196b8be0a19d219e1980d8bc8517a29a678`。
- **KV/Txn A124 non-key NONE sort limit lookahead（2026-07-18）**：对照
  `/root/etcd/server/etcdserver/txn/range.go:rangeLimit`、`sortRangeResults` 与
  `asembleRangeResponse`，新增 raw protobuf Range 选项组合双端矩阵。矩阵覆盖 revision
  filter 先于 Limit、过滤后的 CountOnly 忽略 Limit、矛盾 filter、VALUE+NONE 默认升序、
  KeysOnly 按原 value 排序、version 降序，以及 staged Put/Update/Delete 后的 Txn
  Range；逐项比较有序 key/value/version、Count、More 和事务 revision 关联。

  差分发现 etcd 的一个非直觉契约：当 SortTarget 不是 KEY、SortOrder 为 NONE 且 Limit
  为正数时，etcd 不做全量排序，而是先按 key 读取 `Limit+1` 个候选，再把 NONE 转为
  ASCEND 排序并截断。KubeBrain unary Range 原先只取 `Limit` 个候选，返回 `b,a` 而非
  reference 的 `c,b`；staged Txn Range 原先全量排序，返回 `e,c` 而非 `c,d`。现请求
  翻译层精确增加一个候选，staged executor 在保留完整 Count 后裁出同样的候选窗口；
  显式排序和 revision filter 仍保持全量物化，普通 KEY 分页路径不变。两条确定性回归
  连续 30 轮通过。

  临时 `/root/etcd` 3.8.0-alpha.0 reference 与在线三副本 TiKV-backed KubeBrain
  完整矩阵连续 10 轮、race 3 轮通过；完整 compat suite 用时 69.078s，
  `go test ./...`、根模块与 compat module `go vet ./...` 及强制 `-count=1` 完整
  server race（213.866s）通过。部署 `kubebrain:a124-range-none-lookahead` 后三 Pod
  Ready、zero restart，`/health` 与 `/readyz` 正常，运行时 exact image
  `442fcbe19bcd6c7671f16867178ffddf6f45d182e8a5e12c77b0f352e0a2458a`。
- **KV/Txn A125 MaxInt64 Limit boundary and sort-target expansion
  （2026-07-18）**：继续对照
  `/root/etcd/server/etcdserver/txn/range.go:rangeLimit`，补齐 KEY、VALUE+NONE 和
  staged Txn Range 的 `Limit=math.MaxInt64` 双端矩阵，并将 A124 的非 KEY/NONE
  候选窗口扩展到 CREATE、MOD target。seed 创建顺序与 key 顺序相反，确保测试能区分
  “先取 `Limit+1` 个 key 候选再排序”和全量排序，而不是偶然得到相同结果。

  黑盒验证确认修复前 MaxInt64 结果已与 etcd 一致：KubeBrain 的 int64 `limit++`
  溢出成负数后被 scanner 当作不限。该结果依赖有符号溢出这一隐式副作用，不适合作为
  生产契约；现与 etcd 一样仅在 `0 < Limit < MaxInt64` 时增加 More 探测候选，
  MaxInt64 明确按不限处理。MaxInt64 单元回归连续 30 轮通过，扩展后的完整矩阵在临时
  `/root/etcd` 3.8.0-alpha.0 reference 与在线三副本 TiKV-backed KubeBrain 间连续
  10 轮、race 3 轮通过。

  完整 compat suite 用时 70.800s，backend suite 连续 3 轮用时 125.130s；
  `go test ./...`、根模块与 compat module `go vet ./...`、backend race（52.826s）
  及强制 `-count=1` 完整 server race（219.657s）通过。部署
  `kubebrain:a125-range-limit-boundary` 后三 Pod Ready、zero restart，`/health`
  与 `/readyz` 正常，运行时 exact image
  `3a91359a4dcb283fa5b040c116f265be6cc5e977fe294f495e9baea336dbe377`。
- **Maintenance A126 HashKV revision boundary differential（2026-07-18）**：
  对照 `/root/etcd/server/storage/mvcc/kvstore.go:hashByRev`，补齐 raw
  Maintenance `HashKV` 的 revision 边界双端矩阵。矩阵覆盖 `-1`、`0`、当前写入
  revision、未来 revision 和 `math.MaxInt64`：负 revision 被接受并返回
  `HashRevision=-1`，其空 revision window 哈希在双方均精确为 `0x40a4756d`；
  `0` 表示 latest，当前 revision 精确回显请求 revision，未来值与 MaxInt64 均返回
  gRPC `OutOfRange` 和完整消息
  `etcdserver: mvcc: required revision is a future revision`。

  reference 与在线 KubeBrain 拥有独立的全局 revision 和 compact 历史，因此测试
  比较请求与响应间的不变量，并验证 `CompactRevision` 为 etcd 合法的 `-1` 或正值，
  不错误比较两套数据库的绝对 revision。矩阵在临时 `/root/etcd`
  3.8.0-alpha.0 reference 与在线三副本 TiKV-backed KubeBrain 间连续 10 轮、
  race 3 轮通过；完整 compat suite 用时 74.991s，`go test ./...`、根模块与 compat
  module `go vet ./...`、backend race（52.834s）及完整 server race（202.804s）
  通过。本轮未发现实现差异，只增加兼容性测试；在线集群继续运行
  `kubebrain:a125-range-limit-boundary`，运行时 exact image 仍为
  `3a91359a4dcb283fa5b040c116f265be6cc5e977fe294f495e9baea336dbe377`。
- **Maintenance A127 Alarm GET linearizable header（2026-07-18）**：对照
  `/root/etcd/server/etcdserver/v3_server.go:Alarm`、
  `/root/etcd/server/etcdserver/apply/backend.go:Alarm` 与
  `/root/etcd/server/etcdserver/api/v3rpc/maintenance.go:Alarm`，新增 raw gRPC
  `Alarm(GET)` 双端矩阵，覆盖 NONE、NOSPACE、CORRUPT、未知 AlarmType 和
  `MemberID=math.MaxUint64`。空告警实例上双方均返回空集合且接受所有过滤形状；
  `ACTIVATE/DEACTIVATE` 仍按既定 DBaaS 边界由 TiKV/PD 控制面替代。

  差分发现 reference 的 Alarm GET 经 Raft request 线性化，刚完成 Put 后响应
  Header.Revision 等于当前 MVCC revision；KubeBrain 原先未执行 read barrier，
  请求落到 follower 时可能成功返回滞后的本地 revision。现 GET 在任意有效身份
  鉴权后先执行 `SyncReadRevision`，同步失败返回可重试错误，不再暴露陈旧 header；
  mutation 路径的 root 鉴权和 Unimplemented 契约不变。确定性 diagnostics 与
  read-barrier 回归连续 30 轮通过，临时 `/root/etcd` 3.8.0-alpha.0 reference
  与在线三副本 TiKV-backed KubeBrain 差分连续 10 轮、race 3 轮通过。

  完整 compat suite 用时 75.319s，`go test ./...`、根模块与 compat module
  `go vet ./...`、backend race 及完整 server race（217.107s）通过。部署
  `kubebrain:a127-alarm-header` 后三 Pod Ready、zero restart，`/health` 与
  `/readyz` 正常，运行时 exact image
  `3dbda65560a17ecb35e7676aafc5bb2aeeb63c8dc107a27c7e502cf4527d3a25`。
- **KV A128 standalone DeleteRange boundary differential（2026-07-18）**：
  对照 `/root/etcd/server/etcdserver/api/v3rpc/key.go:checkDeleteRequest`、
  `/root/etcd/server/etcdserver/txn/delete.go:deleteRange` 和
  `/root/etcd/server/storage/mvcc/kvstore_txn.go:deleteRange`，补齐 standalone
  raw gRPC DeleteRange 边界双端矩阵。矩阵验证 `RangeEnd={0}` 从起始 key 删除
  到键空间末尾，`RangeEnd==Key` 与 `RangeEnd<Key` 均为空区间；from-key 删除的
  `Deleted`、有序 `PrevKvs`、共享删除 revision 和最终剩余 key 与 reference
  一致，两个空区间不推进 revision、不返回 PrevKV 且保持全部 seed key。

  from-key 会影响起始 key 之后的整个键空间，因此测试沿用 64 字节 `0xff` 前缀的
  近上界专用空间，避免污染共享在线实例；每个场景结束后再用有界 prefix 清理。
  临时 `/root/etcd` 3.8.0-alpha.0 reference 与在线三副本 TiKV-backed
  KubeBrain 间连续 10 轮、race 3 轮通过，未发现新实现差异。完整 compat suite
  用时 73.911s，`go test ./...`、根模块与 compat module `go vet ./...`、强制
  backend race（52.537s）及完整 server race（206.960s）通过。本轮只增加兼容性
  测试，不改变服务二进制；在线集群继续运行 `kubebrain:a127-alarm-header`，
  exact image 仍为
  `3dbda65560a17ecb35e7676aafc5bb2aeeb63c8dc107a27c7e502cf4527d3a25`。
- **Lease A129 signed ID TTL/List boundary differential（2026-07-18）**：
  审计 `/root/etcd/server/etcdserver/v3_server.go:LeaseTimeToLive`、
  `LeaseLeases` 与
  `/root/etcd/server/etcdserver/api/v3rpc/lease.go:LeaseTimeToLive` 后，新增 raw
  Lease gRPC 双端矩阵。矩阵以显式 ID `-1`、`math.MinInt64` 和
  `math.MaxInt64` 分别 Grant 300 秒租约并绑定 key，验证 Grant 不推进用户 MVCC
  revision、TTL 响应回显完整 signed ID、剩余 TTL 位于合法区间、GrantedTTL
  精确为 300、`Keys=false` 不返回 key、`Keys=true` 返回唯一 attachment，以及
  LeaseLeases 包含该 ID。Revoke 后同 ID 的 TTL 请求仍成功返回 `TTL=-1`、
  `GrantedTTL=0`、空 Keys 和不落后于绑定 Put 的 header。

  同时确认 `LeaseCheckpointRequest` 仅属于 etcd InternalRaftRequest，不是公开
  Lease gRPC；KubeBrain 以持久化 remaining-TTL checkpoint 和换主恢复提供架构
  等价能力，不伪造客户端 RPC。临时 `/root/etcd` 3.8.0-alpha.0 reference 与
  在线三副本 TiKV-backed KubeBrain 间连续 10 轮、race 3 轮通过，未发现新实现
  差异。完整 compat suite 用时 75.549s，`go test ./...`、根模块与 compat
  module `go vet ./...`、强制 backend race（51.819s）及完整 server race
  （206.392s）通过。本轮只增加兼容性测试，不改变服务二进制；在线集群继续运行
  `kubebrain:a127-alarm-header`，exact image 仍为
  `3dbda65560a17ecb35e7676aafc5bb2aeeb63c8dc107a27c7e502cf4527d3a25`。
- **Watch A130 start revision boundary differential（2026-07-18）**：对照
  `/root/etcd/server/etcdserver/api/v3rpc/watch.go` 与
  `/root/etcd/server/storage/mvcc/watchable_store.go`，新增 raw 双向 Watch gRPC
  revision 边界矩阵。`start_revision=0` 创建后只接收后续 Put；
  `start_revision=seed revision` 在 Created 后回放该历史 Put；
  `start_revision=current+1` 允许创建并等待下一 revision；`math.MaxInt64` 同样
  创建成功，普通 Put 不会被提前交付，显式 Cancel 随后返回空 CancelReason。
  矩阵同时固定 Created header 不早于创建前基线、事件 ModRevision 等于对应写
  revision，以及 MaxInt64 watch 的 cancel header 不早于测试 Put。

  临时 `/root/etcd` 3.8.0-alpha.0 reference 与在线三副本 TiKV-backed
  KubeBrain 间连续 10 轮、race 3 轮通过，未发现新实现差异。完整 compat suite
  用时 76.408s，`go test ./...`、根模块与 compat module `go vet ./...`、强制
  backend race（50.975s）及完整 server race（205.314s）通过。本轮只增加兼容性
  测试，不改变服务二进制；在线集群继续运行 `kubebrain:a127-alarm-header`，
  exact image 仍为
  `3dbda65560a17ecb35e7676aafc5bb2aeeb63c8dc107a27c7e502cf4527d3a25`。
- **Compact A131 revision/physical error boundary differential
  （2026-07-18）**：对照
  `/root/etcd/server/storage/mvcc/kvstore.go:updateCompactRev` 与
  `/root/etcd/server/etcdserver/v3_server.go:Compact`，新增 raw gRPC Compact
  边界矩阵。为消除 fresh reference 与长期在线 KubeBrain 的既有 compact history
  差异，场景先分别 compact 到各端自身 seed revision，再请求 logical/physical
  `revision=0`、physical `revision=-1` 以及 logical/physical
  `revision=math.MaxInt64`。零值与负值均精确返回 gRPC `OutOfRange`、
  `etcdserver: mvcc: required revision has been compacted`；MaxInt64 均返回
  `OutOfRange`、`etcdserver: mvcc: required revision is a future revision`。
  Physical 标志不改变校验优先级或错误文本。

  临时 `/root/etcd` 3.8.0-alpha.0 reference 与在线三副本 TiKV-backed
  KubeBrain 间连续 10 轮、race 3 轮通过，未发现新实现差异。完整 compat suite
  用时 79.001s，`go test ./...`、根模块与 compat module `go vet ./...`、强制
  backend race（52.060s）及完整 server race（235.903s）通过。本轮只增加兼容性
  测试，不改变服务二进制；在线集群继续运行 `kubebrain:a127-alarm-header`，
  exact image 仍为
  `3dbda65560a17ecb35e7676aafc5bb2aeeb63c8dc107a27c7e502cf4527d3a25`。
- **KV/Txn A132 Range negative revision and FirstRev boundary
  （2026-07-18）**：对照
  `/root/etcd/server/etcdserver/txn/range.go:checkRange` 与实际 raw gRPC
  路径，补齐负 revision、`math.MaxInt64`、point/range、CountOnly、KeysOnly
  及 Txn 已选/未选分支的双端矩阵。一元 Range 会把任意负 revision 归一为 latest，
  即使已有 compact history 仍成功；Txn 内嵌 Range 保留原始 revision，并按
  `FirstRev` 校验。fresh store 的 `FirstRev=-1`，因此 `revision=-1` 合法、
  `revision=-2` 返回 compacted；完成逻辑压缩后 `-1` 也返回 gRPC
  `OutOfRange` 和
  `etcdserver: mvcc: required revision has been compacted`。未选中的 Txn 分支
  不提前校验，MaxInt64 仍返回 future revision。

  差分发现 KubeBrain 原先只校验正 revision，导致已选 Txn 分支中的负 revision
  在 compact 后仍成功。现 Txn revision validator 按 compact 状态实现等价
  `FirstRev` 边界，同时保持一元 Range 的负值归一化行为。确定性单元回归连续
  30 轮通过；临时 `/root/etcd` 3.8.0-alpha.0 reference 与在线三副本
  TiKV-backed KubeBrain 的 9 场景矩阵连续 10 轮、race 3 轮通过。完整 compat
  suite 用时 90.294s，`go test ./...`、根模块与 compat module `go vet ./...`、
  backend race（52.958s）及完整 server race（236.293s）通过。部署
  `kubebrain:a132-txn-range-first-revision` 后三 Pod Ready、zero restart，
  `/health` 与 `/readyz` 正常，运行时 exact image
  `33b23b61fdfdd3ecceaa8256502e6ce442c1174be6f9f33b1e5e8e5627c0e001`。
- **Lease A133 Grant TTL/ID boundary differential（2026-07-18）**：对照
  `/root/etcd/server/lease/lessor.go:Grant` 与公开 Lease gRPC，新增 raw
  LeaseGrant 双端矩阵。矩阵覆盖 TTL `math.MinInt64`、`-1`、`0`、`1`、最小值
  `2`、最大值 `9_000_000_000`、最大值加一及 `math.MaxInt64`，同时覆盖显式
  ID、自动 ID 和重复 ID。双方均把小于最小值的 TTL 夹到 2 秒并在响应中回显，
  精确接受最大值；超上限返回 gRPC `OutOfRange`、
  `etcdserver: too large lease TTL`。自动 ID 保证非零，重复显式 ID 返回
  `FailedPrecondition`、`etcdserver: lease already exists`，成功创建的租约均在
  场景结束时撤销。

  临时 `/root/etcd` 3.8.0-alpha.0 reference 与在线三副本 TiKV-backed
  KubeBrain 间连续 10 轮、race 3 轮通过，未发现新实现差异。完整 compat suite
  用时 84.310s，`go test ./...`、根模块与 compat module `go vet ./...`、强制
  backend race（54.546s）及完整 server race（241.911s）通过。本轮只增加兼容性
  测试，不改变服务二进制；在线集群继续运行
  `kubebrain:a132-txn-range-first-revision`，三 Pod Ready、zero restart，
  `/health` 与 `/readyz` 正常，exact image 仍为
  `33b23b61fdfdd3ecceaa8256502e6ce442c1174be6f9f33b1e5e8e5627c0e001`。
- **Lease A134 KeepAlive stream/Revoke signed-ID boundary differential
  （2026-07-18）**：对照
  `/root/etcd/server/etcdserver/api/v3rpc/lease.go:leaseKeepAlive` 与公开
  Lease gRPC，新增 13 场景 raw 双端状态机矩阵。同一双向 KeepAlive 流先发送
  ID `0` 和不存在的正 ID，双方均返回匹配 ID、`TTL=0` 的成功响应且不关闭流；
  随后同一流继续成功续租 `-1`、`math.MinInt64` 和 `math.MaxInt64` 三个显式
  租约，返回 TTL 位于 `(0, GrantedTTL]`，response header revision 为正。

  三个 signed ID 首次 Revoke 均成功；再次 Revoke，以及对 ID `0` 和未知 ID
  的 Revoke，均精确返回 gRPC `NotFound`、
  `etcdserver: requested lease not found`。临时 `/root/etcd`
  3.8.0-alpha.0 reference 与在线三副本 TiKV-backed KubeBrain 间连续 10 轮、
  race 3 轮通过，未发现新实现差异。完整 compat suite 用时 87.788s，
  `go test ./...`、根模块与 compat module `go vet ./...`、强制 backend race
  （54.506s）及完整 server race（232.768s）通过。本轮只增加兼容性测试，不改变
  服务二进制；在线集群继续运行 `kubebrain:a132-txn-range-first-revision`，
  exact image 仍为
  `33b23b61fdfdd3ecceaa8256502e6ce442c1174be6f9f33b1e5e8e5627c0e001`。
- **Watch A135 signed/extreme ID and empty-range control differential
  （2026-07-18）**：对照
  `/root/etcd/server/storage/mvcc/watcher.go:watchStream.Watch` 与
  `/root/etcd/server/etcdserver/api/v3rpc/watch.go`，新增单一复用流上的 15 响应
  raw 双端状态机矩阵。显式 watch ID `-1`、`math.MinInt64` 和
  `math.MaxInt64` 均合法创建；其中 `-1` 虽也是错误响应使用的 InvalidWatchID，
  仍可由 `Created/Canceled` 字段无歧义地区分。显式极值不推进自动 ID allocator，
  后续自动创建仍返回 ID `0`。

  重复 MaxInt64 返回 `WatchId=-1`、`Created=true`、`Canceled=true` 和
  `mvcc: duplicate watch ID provided on the WatchStream`；`Key==RangeEnd` 与
  `Key>RangeEnd` 均返回相同控制形状及
  `mvcc: watcher range is empty`。这些错误不会关闭流，后续创建和取消正常；取消
  未知 ID 被静默忽略且不插入额外响应。临时 `/root/etcd`
  3.8.0-alpha.0 reference 与在线三副本 TiKV-backed KubeBrain 间连续 10 轮、
  race 3 轮通过，未发现新实现差异。完整 compat suite 用时 92.868s，
  `go test ./...`、根模块与 compat module `go vet ./...`、强制 backend race
  （52.000s）及完整 server race（221.349s）通过。本轮只增加兼容性测试，不改变
  服务二进制；在线集群继续运行 `kubebrain:a132-txn-range-first-revision`，
  exact image 仍为
  `33b23b61fdfdd3ecceaa8256502e6ce442c1174be6f9f33b1e5e8e5627c0e001`。
- **Cluster A136 complete static membership / official client Sync
  （2026-07-18）**：修复前开发环境虽有 3 个 Ready Deployment Pod，
  `MemberList` 只能从本地配置推导当前成员与 leader，实际仅返回 2 个成员。生产
  manifest 已使用 3 副本 StatefulSet、headless peer Service 和完整
  `--initial-cluster`；本轮把开发 manifest 对齐到相同拓扑，为
  `kubebrain-{0,1,2}.kubebrain-peer.kubebrain-dev.svc` 提供稳定 peer/client
  地址，并增加 manifest 回归测试，防止再次部署无完整成员配置的多副本
  Deployment。

  新增 `MemberListRequest.Linearizable=false/true` raw 双端差分，验证 header
  revision 为 0、cluster/member ID 非零、raft term 为正、当前成员与 leader
  均在列表内，且成员具备完整 name、peer URL、client URL；reference etcd 返回
  1 个成员，KubeBrain 精确返回配置的 3 个成员。连续 10 轮及 race 3 轮通过。
  官方 client `Sync` 测试分层为：宿主机验证同步结果精确等于公告地址后恢复
  NodePort 入口继续数据面 smoke；Pod 内设置
  `KUBEBRAIN_MEMBERLIST_ENDPOINTS_DIALABLE=1`，连续 10 轮直接拨通同步后的
  `.svc` 地址。跨成员 `Status/HashKV` 一致性也仅在公告地址可拨通的环境启用，
  Pod 内连续 10 轮通过，覆盖全部 3 个 serving member。

  完整 compat suite 用时 101.980s，`go test ./...`、根模块与 compat module
  `go vet ./...`、backend race（53.919s）及完整 server race（222.673s）通过。
  StatefulSet 为 3/3 Ready、3 updated、zero restart，`/health` 与 `/readyz`
  正常，`MemberList` 精确返回 3 个稳定成员。本轮不改变服务二进制，继续运行
  `kubebrain:a132-txn-range-first-revision`，exact image 为
  `33b23b61fdfdd3ecceaa8256502e6ce442c1174be6f9f33b1e5e8e5627c0e001`。
- **Put/Lease A137 hot-key coordination and lease-read boundaries
  （2026-07-18）**：新增 raw Lease read 双端矩阵，对照
  `/root/etcd/server/etcdserver/api/v3rpc/lease.go:LeaseTimeToLive`、
  `/root/etcd/server/etcdserver/v3_server.go:LeaseLeases` 与
  `/root/etcd/server/lease/lessor.go:Leases`。矩阵固定 ID `0` 在
  `Keys=false/true` 下均返回成功、`ID=0`、`TTL=-1`、`GrantedTTL=0` 和空 keys；
  活租约在 keys=false 时省略附件、keys=true 时返回完整无重复集合，TTL 位于
  `(0, GrantedTTL]`；LeaseLeases 包含活租约且所有 ID 非零。三类 read header
  均要求 cluster/member ID、revision、raft term 为正。附件 keys 来自集合，reference
  wire 顺序不稳定，因此差分按集合比较，不把 KubeBrain 的确定性排序误写成 etcd
  契约。临时 `/root/etcd` 3.8.0-alpha.0 reference 与在线 KubeBrain 间普通
  10 轮、race 3 轮通过。

  该边界测试本身未发现 Lease 差异，但完整 compat suite 在无并行 race 负载时再次
  复现 `TestConcurrentPutSameKeyNeverFails`：16 writers 对同一 key 的 640 次
  unconditional Put 中 1 次超过 15 秒 caller deadline，单轮约 35 秒。根因是
  KubeBrain 用 Get→CAS 模拟 unconditional Put；同 leader 上的并发请求会把一个热点
  key 放大为大量失败 TiKV 事务、已消费 MVCC revision 和 committed-watermark
  等待。etcd 的 Raft apply 天然串行这些写。现 backend shim 使用固定 256 槽、
  FNV-1a key stripe 的 context-aware 协调器，只在现有 Put Get→Create/Update
  重试循环外串行同槽请求；等待响应 caller cancel，固定数组不会产生动态 key
  lock map 泄漏，不同槽位仍并行。确定性测试覆盖同 key 最大并发为 1、不同槽位
  可并行以及 canceled waiter 立即退出，focused 普通 50 轮、race 10 轮通过。

  A137 镜像滚动后热点测试连续 10 轮、共 6400 次 Put 全部通过，用时
  135.990s，平均每轮约 13.6 秒；完整 compat suite 用时 88.106s。
  `go test ./...`、根模块与 compat module `go vet ./...`、backend race
  （58.192s）及完整 server race（239.958s）通过。最终 StatefulSet 3/3 Ready、
  3 updated、zero restart，`/health`、`/readyz` 与完整 3-member MemberList
  正常；滚动后无 TSO deadline、still-contended、Panic/Fatal 日志。运行镜像
  `kubebrain:a137-put-key-coordination`，exact image 为
  `c78e30bf5f6b217ad9f7bdf0efc7ed3193d9dbeede93cf43596a3c53d9f6dbb3`。
- **Mutation A138 point/range delete and Txn conflict coordination
  （2026-07-18）**：沿 A137 审计其余公开写入口发现，单键 DeleteRange 在
  Get→CAS 冲突后把 backend `Succeeded=false` 直接映射为 `Deleted=0`；对于请求
  开始前已存在、期间只有 Put 的 key，这会把“版本被并发覆盖”伪装成“键不存在”。
  原子 TxnApply 虽会在 compare guard conflict 后重试，但与独立 Put/Delete 没有
  共同协调边界，热点 key 仍会消耗失败 TiKV 事务和 MVCC revision。范围 Delete
  也曾在无排他 barrier 的 List 快照后提交 guards，允许并发 phantom Put 迫使整笔
  请求失败。

  现把 A137 的 Put-only 256-slot FNV-1a stripe 扩展为 context-aware mutation
  coordinator。单 key Put/Delete 共用零额外集合分配的 fast path；原子 TxnApply
  对写 key 去重并按 stripe 升序获取，反向释放，避免多键反序死锁；范围 Delete
  的目标 key 集合也在读取 current revision guards 前通过 `BeginMutation` 取得全部
  stripe，并把 ownership 写入 context；后续 TxnApply 识别已持有槽而不重入死锁。
  BackendShim 的直接 range-delete 路径则从 List 到单 revision commit 全程复用已有
  `BeginRangeTxn` 全局写 barrier。等待任一槽时 caller cancel 会释放已取得槽且不进入
  backend，固定数组仍无动态 key-lock 生命周期问题。确定性 fake backend 回归预占
  目标槽并取消 Delete/Txn caller，要求二者返回 `context.Canceled` 且 backend 调用
  计数为 0；另一路回归要求 BeginMutation context 内的 TxnApply 一秒内完成，防止
  ownership 重入死锁。旧路径会越过协调层直接调用 backend。协调 focused 普通
  50 轮、race 10 轮以及 Delete/Txn/Range focused 20 轮通过。

  官方 client/v3 黑盒固定两类并发契约：预置 key 后每轮并发 8 Put 与单 Delete，
  Delete 在任一可线性化位置都必须返回 `Deleted=1`；8 writers 对同一 key 的
  无 compare Txn 必须始终选择 Success 且不暴露 guard/CAS 错误。reference etcd
  两类合并连续 10 轮通过。最终 A138 在线镜像上 640-write hot Put、Put/Delete
  race 与无条件 hot Txn 三类契约合并连续 3 轮全部通过，用时 59.508s；同 key
  冲突被协调，不同 stripe 仍并行。

  完整 compat suite 用时 91.776s，`go test ./...`、根模块与 compat module
  `go vet ./...`、backend race（57.628s）及完整 server race（259.628s）通过。
  最终 StatefulSet 3/3 Ready、3 updated、zero restart，`/health`、`/readyz`
  与完整 3-member MemberList 正常；滚动后无 TSO deadline、contention、
  Panic/Fatal 日志。运行镜像 `kubebrain:a138-mutation-window`，exact image
  为 `ebb6db1d3449c60c90afd068ac4af554ac194fe0225b29c22ee54a332bc0a1f8`。
- **Mutation/Proxy A139 backend failover recovery（2026-07-18）**：新增 opt-in
  官方 client/v3 破坏性黑盒测试。测试以 6 workers、每 worker 30 次操作持续对
  同一热点 key 混合 Put、Delete 与无 compare Txn，并通过
  `KUBEBRAIN_MUTATION_FAILOVER_COMMAND` 删除唯一 PD Pod。故障窗口只接受 caller
  cancel/deadline、gRPC Canceled/DeadlineExceeded/Unavailable，以及 TiKV PD
  client 原样透出的精确 `ErrClientTSOStreamClosed`；任意其他 Unknown 仍立即失败。
  测试要求至少出现一次提交结果不确定，并在 45 秒内重新完成
  Put→unconditional Txn→Delete(`Deleted=1`)，从外部证明 mutation stripe 未泄漏。

  重复故障验证发现一个产品恢复缺陷：失去领导权的 follower 可把本地 resource-lock
  缓存更新为 `empty`，而 etcd proxy 只读该缓存；当旧 term 初始化仍在退出时，该
  follower 会长期 `/ready=503`，即使共享锁中已有新 leader。现
  `LeaderElection.RefreshLeaderInfo` 只在缓存未知时以有界 context 重读共享选举记录，
  稳态请求仍使用无存储 I/O 的缓存；proxy 后台循环绑定根服务 context，关闭时先停止
  刷新，避免 Badger 已关闭后继续访问。确定性测试固定
  `empty→refresh→connect→Ready` 转换。全仓门禁还发现 revision 测试 cmux/server
  goroutine 在子测试返回后使用 `testing.T`，现纳入 WaitGroup，并保证最后日志先于
  `Done`；该包 race 连续 3 轮通过。

  最终镜像上删除/重建唯一 PD 后，180 次热点混合 mutation 故障测试用时 23.583s
  并恢复；三个 KubeBrain Pod 随即全部 Ready、zero restart，日志显示 follower
  重新连接新 leader，leader 在同一进程中 lost/reacquired。`MemberList` 完整返回
  3 个 started member，`/readyz=ok`，Raft term 从 A138 的 258 增至 270。完整
  compat suite 用时 84.366s，`go test ./...`、根模块与 compat module vet、
  backend race（54.445s）及最终完整 server race（250.162s）通过。运行镜像
  `kubebrain:a139-proxy-refresh`，三 Pod 运行时 exact image digest 均为
  `194da00e9edf2a3c705ea8020c87d76b6526c1322cacfaa7a756775a2767a583`。
- **Backend quorum A140 3 PD/3 TiKV continuous progress（2026-07-18）**：
  把开发验证 TidbCluster 从单 PD/单 TiKV 提升为 3/3，与生产拓扑保持一致；
  PD API 确认 3 个 member 和 3 个 Up store，三个 store 均持有全部 80 region
  的副本，最终 leader 分布为 23/29/28。新增 opt-in 官方 client/v3 故障黑盒：
  8 workers 使用独立 key 持续混合 Put、无 compare Txn 和 Get，外部命令删除
  当前 PD leader 或一个 TiKV member；故障命令运行期间至少要完成每 worker
  一次数据操作，恢复后每个 key 必须在 45 秒内 Put+Get 同值。Kubernetes Pod
  Ready 不再被误当成 region routing 已恢复。`hack/dev/backend-quorum-fault-smoke.sh`
  校验拓扑必须精确为 3/3，并自动执行两类故障。

  首轮 PD leader 删除暴露 TiKV PD client 的
  `ErrClientTSOStreamClosed` 被 gRPC 默认编码为 Unknown。现 TiKV storage 的
  TSO 获取失败保留原始原因并标记为已有 `storage.ErrUnavailable`，统一 gRPC
  interceptor 只把该明确 sentinel 映射为 `codes.Unavailable`，不掩盖其他未知
  程序错误；A139/A140 故障判定器相应移除 Unknown 白名单。最终一键脚本中 PD
  leader 故障窗口完成 1628 次操作，唯一瞬时错误为标准 Unavailable；TiKV member
  故障窗口完成 101 次操作，仅见标准 DeadlineExceeded，均完成恢复复读。

  完整 compat suite 用时 74.893s，`go test ./...`、根模块与 compat module
  vet、TiKV storage race（1.214s）、backend race（52.774s）及完整 server race
  （261.604s）通过。最终 KubeBrain StatefulSet 3/3 Ready、zero restart，
  PD/TiKV StatefulSet 均 3/3 Ready，TidbCluster `Ready=True`，3-member
  MemberList 完整，Raft term 273。运行镜像 `kubebrain:a140-backend-quorum`，
  三 Pod 运行时 exact image digest 均为
  `a6c83f8228c67d49bdfcec607b3d3ca745518212e10f0d961fd4e36222bc92ad`。
- **Restart persistence A141 replicated rolling recovery（2026-07-18）**：
  对齐 etcd `tests/integration/revision_test.go` 的
  `TestRevisionMonotonicWithLeaderRestarts` 与 `TestRevisionMonotonicWithRestarts`，
  新增 opt-in 官方 `client/v3` 黑盒及一键脚本。测试先持久化普通值、删除记录、900
  秒 lease 附属 key 和独立 watch 历史；外部命令运行期间持续 Range 并拒绝任意成功
  响应 revision 回退，恢复后验证当前值、删除前历史值与 tombstone、lease ID/TTL/
  attached keys、从写入 revision 开始的 watch 历史回放，以及下一次 Put revision
  严格增长。

  `hack/dev/restart-persistence-smoke.sh` 只接受精确 3×KubeBrain、3×PD、3×TiKV
  拓扑，按单 Pod 删除、等待同名 Pod Ready 的方式顺序恢复全部 9 个成员，避免把
  Kubernetes Pod 重建等同于数据层恢复。真实集群一轮用时 68.42 秒并完整通过；
  最终 KubeBrain、PD、TiKV StatefulSet 均 3/3 Ready，TidbCluster `Ready=True`，
  3-member MemberList 完整、Raft term 275、`/readyz=ok`，近 10 分钟日志未见
  Panic/Fatal/TSO deadline/contention。完整 compat suite 用时 65.615s，根模块
  `go test ./...`、根模块与 compat module vet、backend race（52.851s）、TiKV
  storage race（1.200s）及完整 server/etcd race（264.163s）通过。该门禁证明
  quorum-preserving 滚动恢复持久性，不替代 BR/PITR、跨可用区分区或多数副本丢失
  演练。运行镜像仍为 `kubebrain:a140-backend-quorum`，三 Pod exact image digest
  均为 `a6c83f8228c67d49bdfcec607b3d3ca745518212e10f0d961fd4e36222bc92ad`。
- **Backup A142 logical artifact integrity（2026-07-18）**：对照 etcd
  `etcdutl/snapshot/v3_snapshot.go` 的 `Status` 与 `copyAndVerifyDB`：官方 restore
  会先验证 snapshot 结构和 SHA-256，再修改目标 DB；原 logical JSONL 没有版本、
  manifest、记录数或摘要，截断文件仍可能被 restore/verify 当成完整输入，造成静默
  缺数。现引入 `kubebrain.logical.v1`：header 固定源 prefix 与 snapshot revision，
  footer 固定总记录数及覆盖 header/全部记录的 SHA-256；同时验证 revision 为正、
  每个 key 位于 manifest prefix 内且无重复 key。单测覆盖截断、内容篡改、footer 后
  追加数据、prefix 不匹配、重复 key，以及 abort 不覆盖已有备份。

  export 在目标同目录写 0600 临时文件，文件 `fsync` 后原子 rename 并同步父目录；
  status/restore/verify 先复制到私有临时文件并完整验证，restore 在验证成功前不创建
  etcd client、不写目标。新增 `logical-status.sh` 返回格式、revision、记录数和摘要；
  `BATCH_SIZE` 现真正使用单个 Txn 提交一批 compare+Put，批内冲突不再逐 key 部分
  落盘。旧无 manifest JSONL 无法证明完整性，明确 fail closed，升级后需重新导出。

  真实 3 PD/3 TiKV 环境中，34 条 `/registry` 在 revision
  `467759280324347731` 导出并以 8 条/Txn 隔离恢复，count/逐值 verify 均为 34 后
  清理；截断 footer 的恢复被拒绝且目标 count=0；两条记录中第二条目标已存在时，
  整批拒绝且 target count 保持原有 1。overwrite guard、恢复后内容篡改和完整性
  smoke 连续通过。完整 compat suite 用时 61.927s，`go test ./...`、全仓与 backup
  vet、backup race（各包约 1.05s）通过。该里程碑只增强对象级逻辑迁移/隔离恢复，
  不保留原 revision/lease/watch 历史；A143 已证明 TiDB BR 全量与 PITR 不能作为
  KubeBrain 主灾备路径。
- **Backup A143 physical restore boundary（2026-07-18）**：在源 3 PD/3 TiKV
  集群 revision `467759280324349580` 写入备份前标记，使用 TiDB Operator v1.6.5、
  BR v8.5.3 和集群内 S3 兼容对象存储执行 full backup。任务成功备份 131 ranges、
  1473 KV、422188 bytes，checksum 通过，commit TS
  `467759848010022914`。随后写入备份后标记，并把该对象恢复到完全独立的
  `kb-restore` PD/TiKV 集群；Restore CR 在 7 秒内 `Complete`。

  连接目标 PD 的隔离 KubeBrain 对两个标记均返回空，尤其备份前标记缺失，实证 TiDB
  BR full 只覆盖 TiDB 元数据所描述的范围，成功状态不代表 KubeBrain transactional
  key 已备份。BR raw CLI 每次只接受一个 `default/write/lock` CF，官方仍标记
  experimental；分别备份 CF 不能提供已验证的跨 CF 事务快照，因此也不冒充生产能力。
  新增 `production-mode-check.sh`：控制面只允许 `logical`，对 `br-full`、`br-pitr`、
  `br-raw` 和未知模式 fail closed；smoke 固定该契约。当前生产 DR 基线是 A142
  checksummed/atomic logical artifact 加隔离恢复与逐值验证。物理 PITR 保持明确缺口，
  直到 TiKV 提供受支持的 transactional key-range snapshot/PITR，或 KubeBrain 引入
  自有一致的跨 CF 物理备份实现。
- **Backup A144 lease-aware logical restore（2026-07-18）**：A142 v1 虽已把
  `mvccpb.KeyValue.Lease` 写入记录，restore 却忽略该字段，导致临时 key 灾备后静默
  变成永久 key。现升级为 `kubebrain.logical.v2`：首次遇到源 lease 时通过官方
  client/v3 `TimeToLive` 读取正数剩余 TTL并写一条去重 lease metadata；导出期间 lease
  已过期则 fail closed。footer 同时校验记录数、lease 数和覆盖全部行的 SHA-256，
  validator 拒绝重复/无效 lease 及引用未声明 lease 的记录。

  restore 在写 key 前完成制品验证和 lease 引用验证，为每个源
  lease Grant 一个新的目标 ID；Put 使用 `WithLease`，同源 lease 的多个 key 保持共享，
  不把源 ID 与目标已有 lease 冲突。创建 lease 后发生失败会撤销本轮已创建目标 lease。
  verify 同时断言永久 key 仍永久、leased key 非永久、同源 lease 不拆分、不同源 lease
  不合并且目标 TTL 为正。v1 无 lease 制品继续可读；v1 中存在非零 lease 因无法恢复
  TTL，在任何目标写入前拒绝并要求重导。

  真实 3 PD/3 TiKV smoke 使用一个永久 key 和两个共享 120 秒 lease 的 key，v2 导出
  3 records/1 lease，隔离恢复、count、value、lease 等价关系及 TTL 全部通过并清理；
  36 条 `/registry` 全前缀 v2 演练，以及截断、overwrite guard、恢复后内容篡改 smoke
  同轮通过。该能力恢复 snapshot 时剩余 TTL，不保留原 lease ID、原 create/mod
  revision、version 或 watch 历史。
- **Backup A145 restore preflight and Txn budget（2026-07-18）**：对照 etcd
  `server/etcdserver/api/v3rpc/key.go:checkTxnRequest`，`max-txn-ops` 取
  compare/success/failure 三组长度的最大值而非总和；因此默认 128 compare + 128 Put
  合法。restore 新增 `MAX_TXN_OPS`（默认 128），启动时拒绝更大的 `BATCH_SIZE`，使
  控制面调整实例 `--max-txn-ops` 时必须同步恢复任务配置。

  原 overwrite guard 只证明同一 Txn 批内原子；大制品在后续批发现已有 key 时，前面
  批已写入。现非覆盖 restore 在创建目标 lease 或写 key 前，先按 `BATCH_SIZE` 用只读
  Txn 对全部重写后目标 key 做 existence preflight，网络往返保持 O(N/batch)；正式写批
  继续使用 `Version(key)==0` compare，关闭预检后的并发创建竞争。真实 3 PD/3 TiKV
  guard 将两个记录强制拆成 `BATCH_SIZE=1`，第二个目标预置冲突，恢复拒绝且目标 count
  保持原有 1，证明早期批零写入。单测覆盖 128 边界、129 超限和非法零上限。
- **Backup A146 acknowledged-batch conditional rollback（2026-07-18）**：目标
  preflight 和每批 CAS 不能处理后端在第 N 批返回错误：前 N-1 个已确认批此前会残留。
  现默认非覆盖 restore 保存每个成功 Txn 的 key 集与响应 revision；后续失败时按逆序
  发送同规模 Txn，以每个 key 的 `ModRevision==batchRevision` 为 compare 后 Delete。
  同批 Put 共享一个 revision，compare 任一失败会使整批删除不执行，因此恢复后的并发
  更新不会被误删，错误明确标记 rollback incomplete。

  lease key 先按 revision 删除再撤销本轮目标 lease；`ALLOW_OVERWRITE=true` 因无法用
  Delete 重建被覆盖的旧 value/lease，明确不自动回滚。提交成功但响应丢失的事务不在
  acknowledged 列表中，仍保留为人工核对的不确定结果。新增 test-only
  `FAIL_AFTER_BATCHES` 和 opt-in smoke：3 条记录以 `BATCH_SIZE=1` 恢复，第 2 批提交后
  注入失败，真实 3 PD/3 TiKV 上两个已确认批逆序回滚，目标 count=0。单测固定逆序和
  首个冲突即停止的 fail-closed 行为。

- **Production A147 StatefulSet readiness alert（2026-07-18）**：生产工作负载已从
  Deployment 迁移为 StatefulSet，但 `KubeBrainReadinessUnavailable` 仍查询
  `kube_deployment_status_replicas_available`，导致正常安装中该时间序列不存在、告警
  静默。现改查 `kube_statefulset_status_replicas_ready`，并用
  `or on() vector(0)` 将 kube-state-metrics 缺失同样视为零 ready，避免监控链路故障
  被 PromQL 空向量吞掉；告警文案同步使用 StatefulSet/ready 语义。

  manifest 回归测试按 PrometheusRule 的告警名结构化定位并固定完整表达式，同时固定两
  套生产清单的 RollingUpdate、30 秒 termination grace、三副本和
  `PodDisruptionBudget minAvailable=2` 契约。Prometheus 3.5 `promtool check rules`
  校验通过；真实 kind 环境 KubeBrain、PD、TiKV StatefulSet 均 3/3 Ready 且全部
  updated，TidbCluster `Ready=True`。该环境未安装 Prometheus Operator CRD，因此
  PrometheusRule 的接纳与实际触发仍须在预生产监控栈演练。
- **Production A148 TiKV/PD rollout contract（2026-07-18）**：独立存储生产清单
  原有 3 PD/3 TiKV、PV Retain、资源限额、主机强反亲和及 `minAvailable=2` PDB，但
  组件升级策略和终止宽限期依赖 TiDB Operator 默认值，且没有任何结构化回归测试。现
  显式固定 PD/TiKV `statefulSetUpdateStrategy: RollingUpdate`，PD termination grace
  为 60 秒，TiKV 为 300 秒；原有 TiKV `evictLeaderTimeout: 10m` 继续约束删除前的
  leader 驱逐。

  新增测试固定 TiDB 版本、PV reclaim、动态配置、三副本、failover 上限、CPU/内存/
  存储请求、资源上限、主机反亲和 topology key、PDB selector/minAvailable，以及上述
  滚动和终止契约。当前 Operator CRD 的 server-side dry-run 接纳完整生产清单。真实
  3 PD/3 TiKV kind 环境仅 patch 这四个字段，Operator 先逐成员滚动 PD、再逐成员滚动
  TiKV；最终 TidbCluster `Ready=True`，两个 StatefulSet 均 3/3 ready/current，
  生成模板分别为 60/300 秒，KubeBrain etcd 端点保持可提交请求。TiDB Operator 使用
  rollingUpdate partition 协调升级，不能以普通 `kubectl rollout status` 的
  “partitioned roll out complete” 作为整集群升级完成证据；控制面必须等待
  TidbCluster Ready 及 currentRevision/updateRevision 收敛。
- **Control plane A149 strict TiDB convergence gate（2026-07-18）**：新增
  `hack/production/wait-tidbcluster-ready.sh` 作为发布和配置变更后的控制面门槛。
  它不把 TidbCluster 单独的 Ready condition 当作充分条件；还要求 PD/TiKV
  StatefulSet generation 已观察、desired=ready=updated 且大于零，以及
  currentRevision=updateRevision。超时返回非零并打印 CR/StatefulSet 诊断，context、
  namespace、cluster、timeout 和 poll interval 均可由环境注入。

  回归测试固定完整收敛成功，并证明 Ready=True 但 partition 仅 updated 1/3、generation
  尚未观察或 CR Ready=False 都不能提前成功。真实 3 PD/3 TiKV 集群上等待器确认两组
  StatefulSet revision 收敛并立即返回。生产就绪文档同步把 production manifest 定义
  为需平台注入 StorageClass、镜像、跨 AZ、网络、证书和监控的测试基线，并把等待器及
  后续 endpoint/实际读写检查写入升级门槛。等待器随后补充本地输入门禁：在调用
  kubectl 前拒绝非法 `NAMESPACE`/`TIDB_CLUSTER` DNS label，并用回归测试证明坏参数
  不会触发集群访问。
- **Watch/Lease A150 require-leader admission（2026-07-18）**：对照
  `/root/etcd/server/etcdserver/api/v3rpc/interceptor.go`，官方 client
  `WithRequireLeader` 通过 incoming metadata
  `etcd-server-leader=true` 要求服务端在无 leader 时返回
  `Unavailable: etcdserver: no leader`。KubeBrain 此前完全忽略该 metadata，Range
  等读 RPC 即使调用方显式要求 leader 仍会在选主空窗继续执行。

  public 和 peer gRPC listener 现统一在 handler 前检查 metadata；本机 leader 或已知
  远端 leader 时继续处理，未知 holder 时返回官方 `ErrGRPCNoLeader`。public 拒绝发生
  在 QPS/并发配额之前，内部 peer 转发同样检查但不消耗公共配额。真实 gRPC bufconn
  测试覆盖 unary、stream 建立、无 metadata 放行及 follower 已知远端 leader 放行，
  focused race 和完整 server 测试通过。`kubebrain:a150-require-leader` 在真实
  3 PD/3 TiKV 上滚动为 3/3 Ready、零重启；官方 client/v3 黑盒以
  `WithRequireLeader` 完成 Range、Lease Grant、KeepAliveOnce 和 Watch。
  上游还会在已建立 stream 生命周期中主动感知 leader 丢失并取消；KubeBrain election
  当前没有可订阅的 cluster leader freshness 通知，该动态关闭语义仍作为后续差距，
  不以本项关闭。
- **Watch/Lease A151 require-leader stream lifecycle（2026-07-18）**：闭合 A150
  保留项。选主层现从每次观察到的共享 `LeaderElectionRecord.RenewTime` 计算
  `RenewTime + LeaseDuration`，本机则继续使用更严格的 `RenewDeadline` freshness；
  因此缓存中的旧 holder 地址不会无限期冒充有效 leader。peer service 将该状态提供给
  etcd admission，但没有扩大已有公共接口和测试替身的兼容负担。

  仅带 `etcd-server-leader=true` 的 stream 每 100ms 检查 cluster leader freshness；
  holder lease 过期时取消 handler context 并返回官方 `ErrGRPCNoLeader`，普通 stream
  无额外 goroutine/ticker。单测固定有效/过期 election record、本机 fresh leader，
  以及已建立 gRPC stream 在状态切换后主动关闭；focused race 和完整 leader/server
  测试通过。`kubebrain:a151-require-leader-loss` 真实 3 PD/3 TiKV 三副本滚动后，
  官方 client/v3 require-leader Range/Lease/KeepAliveOnce/Watch 连续 100 轮期间删除
  当前 leader Pod；客户端仅见两次可重试 Unavailable，最终 100 轮全通过并恢复 3/3。
- **Production A152 traceable container builds（2026-07-18）**：实际 A150/A151
  Docker 构建日志显示 `.dockerignore` 排除 `.git` 后，旧 `build-base.sh` 的 git
  命令失败却继续编译，镜像中 `Version`/`Git SHA` 为空，无法证明运行副本来自哪个
  commit。现 Dockerfile 要求 CI 显式注入 version、40 位完整十六进制 commit SHA 和
  UTC build date；缺字段在编译前 fail closed。二进制 ldflags 与 OCI
  `org.opencontainers.image.version/revision/created` labels 使用同一输入，开发
  `up.sh` 从宿主 git 注入，直接本地 build 脚本仍可回退读取 git。

  构建契约测试覆盖显式值进入 ldflags、缺失值拒绝和缩写 SHA 拒绝；生产就绪文档新增
  CI 命令及 label/二进制交叉核验门槛。带测试 metadata 的实际 TiKV 镜像构建成功，
  `docker inspect` 与容器内 `kube-brain version` 三项完全一致；最终提交镜像继续以
  本提交 SHA 重建并滚动验证。
- **gRPC A153 client API version metadata（2026-07-18）**：继续对照
  `/root/etcd/server/etcdserver/api/v3rpc/interceptor.go`，补齐
  `client-api-version` incoming metadata 验证。public/peer 的 unary/stream 均在
  handler 和公共 QPS/并发配额之前检查第一个值；缺失或合法 UTF-8 放行，非法 UTF-8
  返回官方 `ErrGRPCInvalidClientAPIVersion`，避免代理链把畸形版本传播到日志和指标。

  直接 incoming-context gRPC interceptor 测试覆盖四条路径、handler 未执行及配额未
  消耗。当前 grpc-go 会在客户端发送前先拒绝非打印 ASCII，因此标准网络客户端观察到
  `Internal: header key ... contains value with non-printable ASCII characters`，不会到达
  server-side UTF-8 分支；新增参考 etcd/KubeBrain 双端差分固定缺失/合法成功和该传输
  错误，连续 10 轮通过。server-side 官方 InvalidArgument 分支仍用于可注入原始 metadata
  的代理、其他传输实现及未来 grpc-go 行为。
- **KV A154 RangeStream validation order（2026-07-18）**：逐项对照
  `/root/etcd/server/etcdserver/api/v3rpc/key.go` 的独立 KV 请求校验，确认
  Range/Put/DeleteRange 的空 key、非法 sort enum、IgnoreValue/value 和
  IgnoreLease/lease 冲突均已匹配；审计发现 RangeStream 同时携带自定义排序与
  revision filter 时错误优先级相反。现按 etcd 顺序先检查排序，再检查 revision
  filter，因此组合非法请求稳定返回
  `Unimplemented: RangeStream does not support custom sort orders`。

  新增真实 gRPC 差分固定空 key、两个非法 sort enum、自定义排序、revision filter
  及组合请求共六种结果。修复前 A153 仅组合请求与参考 etcd 不同；修复后单元回归、
  focused race、主模块全量测试通过，并在真实 3 PD/3 TiKV、三副本 KubeBrain 上
  通过六类 validation 参考差分连续 10 轮、五类 RangeStream 行为差分和大消息分块
  测试。兼容模块无 endpoint 的全量运行会连接默认 `127.0.0.1:3379`，不作为有效证据。
- **KV A156 Put lease error precedence（2026-07-18）**：对照
  `/root/etcd/server/etcdserver/txn/put.go:checkPut` 发现，etcd 在读取
  IgnoreValue/IgnoreLease 所需旧 KV 前先验证请求显式 lease。A154 对“缺失 key +
  IgnoreValue + 不存在 lease”错误地先返回
  `InvalidArgument: etcdserver: key not found`，参考 etcd 返回
  `NotFound: etcdserver: requested lease not found`。

  standalone Put 的 `putWithEffectiveOptions` 和 Txn staged executor 现均先校验显式
  lease，再解析旧值；解析后仍校验最终继承的 lease，避免把已失效绑定重新写回。
  单测同时覆盖 Put 与选中 Txn 分支，现有 Put 差分和 Txn operation validation
  差分加入该组合。修复前真实 A154 双端差分只在此字段失败；focused race 和主模块
  全量测试通过，最终镜像继续在 3 PD/3 TiKV 三副本上验证。
- **KV A157 selected Txn validation order（2026-07-18）**：继续对照
  `/root/etcd/server/etcdserver/txn/txn.go:checkTxn`，etcd 在执行任何写入前按选中
  分支的操作顺序交错检查 Put lease/Ignore* 与 Range revision。KubeBrain 此前先全局
  扫描全部 Range revision，导致 `missing lease Put -> future Range` 错误地返回
  future revision；参考 etcd 返回前一个操作的 LeaseNotFound。反向
  `future Range -> missing lease Put` 两端均返回 future revision。

  现新增 selected-branch 前序预检，按操作顺序检查 Range compact/future、Put 显式
  lease、Ignore* 旧 key，并递归保持 nested Txn 顺序；执行/提交前的二次 lease 校验
  继续防止预检后的状态变化。readonly Txn 与 ordered write Txn 共用单 Range revision
  helper。双向顺序单测、focused race 和主模块全量测试通过；真实参考差分固定两种
  顺序，最终镜像继续在 3 PD/3 TiKV 三副本上验证。
- **KV A158 historical CountOnly More（2026-07-18）**：新增 standalone Range
  确定性组合矩阵，以唯一 value/version/create/mod 的六个 key 覆盖 current 与
  historical revision、五种 sort target、三种 order、Limit 0/2、四组 revision
  filter 及 full/KeysOnly/CountOnly，共 720 个双端请求。current 的 360 组全部一致；
  historical 矩阵发现 CountOnly+Limit 经 List fallback 后错误保留底层分页
  `More=true`，参考 etcd 始终为 false。

  `applyRangeOptions` 现于 CountOnly 最终整形时同时清空 KVs 和 More；Limit 仍不截断
  Count，普通 range 的 More 不变。单测明确构造更新后的历史快照并断言
  `Count=3, KVs=[], More=false`；focused race、主模块全量及最终 720 组参考差分通过，
  最终镜像继续在 3 PD/3 TiKV 三副本上验证。
- **KV A159 binary key range ordering（2026-07-18）**：将 standalone Range
  矩阵扩展到 negative/MaxInt64 revision filter、Limit -1/0/2/MaxInt64 和删除后重建
  key 的 create/version 重置，current/historical 共 2880 组与参考 etcd 一致。另以
  `00`、嵌入 NUL、`ff` 及其后缀构造原始字节键，发现内部
  `user-key + '$' + revision` 编码不能直接表示低于分隔符的范围边界，且此前把任意
  尾随 NUL 当作 Kubernetes pagination，导致 `[00,01)` 和 `ff` FromKey 漏键。

  普通 ASCII/prefix 范围继续使用原有 TiKV 有界扫描；仅 FromKey 或包含不安全低字节
  的边界走解码后的用户键过滤与字节序排序回退，并由 List、Count、RangeStream 共用。
  单测固定 NUL 区间和 `ff` FromKey 的 Count/More/Limit；真实 gRPC 差分同时覆盖 unary、
  historical delete point 及 RangeStream。该回退当前需物化租户完整 keyspace，属于
  通用二进制兼容的正确性路径，后续容量优化不得改变用户键字节序语义。
- **KV A160 boundary-prefix binary ordering（2026-07-18）**：继续扩展原始字节键
  差分后发现 A159 的静态判定仍不充分：即使请求边界 `fe`/`ff` 自身没有低字节，
  内部 `{user-key}$revision` 排序也会把 `fe00`/`fe01` 放到 `fe` 的版本行之前，
  并把 `ff00`/`ff01` 放到 `ff` 边界之前。修复前 `[fe,ff)` 因而错误返回
  `fe,ff00,ff01`；standalone DeleteRange 只报告并删除 `fe`，而参考 etcd 返回并删除
  `fe,fe00,fe01`。Txn 的低字节 `[00,01)` 路径因 A159 已走全扫描而正确。

  现于普通高字节 start/end 两侧先执行极窄的内部边界邻域探测
  `boundary+[00,'$')`；实际发现错排扩展键时才切换到解码用户键的完整过滤与排序。
  边界自身含低字节仍直接回退。该选择逻辑由 List、Count、RangeStream 共用，因此
  unary、stream、Txn Range、Txn/standalone DeleteRange 保持同一字节序语义；常见
  Kubernetes ASCII/prefix 范围没有低字节扩展键，继续使用 TiKV 有界扫描而非物化
  整个租户。单元测试固定 `[fe,ff)` 的 Range、Deleted=3 和三项 PrevKV，新增真实
  gRPC mutation 差分覆盖 readonly Txn、Txn DeleteRange、standalone DeleteRange
  及删除后状态。
- **KV A161 boundary probe latency（2026-07-18）**：A160 在真实 TiKV 上的 2880
  组矩阵从 A159 的约 19–23 秒增至约 37 秒，确认串行 start/end 邻域探测带来不可忽略
  的读延迟。两侧探测现于同一固定 revision 并行执行；任一侧命中即可立即选择解码
  回退，均未命中时则要求两侧都成功后才进入原有有界扫描。该优化不缓存跨 revision
  结果，不会让并发写后的新低字节扩展键被旧 negative cache 隐藏。真实 TiKV 上相同
  2880 组矩阵两轮降至约 29.9/31.2 秒，较 A160 改善但仍高于 A159；二进制
  unary/RangeStream/Txn/standalone mutation 参考差分脱离并发压测后连续 10 轮通过。
- **Watch A162 binary prefix cold-history replay（2026-07-18）**：沿 A160 的编码
  根因审计重启恢复路径，发现实时 Watch 使用原始用户键前缀路由而不受影响，但 event
  log 不可服务时的存储历史回放仍扫描
  `[Encode(prefix), Encode(PrefixEnd(prefix)))`。该范围会漏掉 `prefix+00/01`；
  arbitrary range Watch 使用空后端前缀时，`PrefixEnd(empty)={0}` 还会形成错误的
  空历史范围。因此 Pod 重启、冷缓存或 event-log watermark 之前的历史 Watch 可能
  静默缺事件。

  历史回放现以原始 `magic+prefix+00` 为起点；空 prefix 从租户 object keyspace
  起点开始，无可用 prefix end 时止于租户边界。解码后再用 `bytes.HasPrefix` 排除
  end 边界低字节扩展和内部 event-log/internal KV family。一次 Iter、每键版本连续、
  DELETE PrevKV 恢复和 reconnect-herd singleflight 不变量保持不变。单元测试固定
  `fe`、`fe00`、`fe01` 与 `ff00` 的前缀/全范围回放，并将真实全副本重启测试扩展为
  重启后从首个 revision 恢复三个二进制前缀事件。
- **Observability A163 binary label safety（2026-07-18）**：A162 真实重启验证虽然
  正确恢复三个二进制历史事件，但 Watch channel 关闭时把原始非 UTF-8 prefix 作为
  Prometheus label，client_golang 在 `CounterVec.With` 中 panic，造成一个 KubeBrain
  容器以 exit 2 重启。该问题证明通用字节键兼容必须覆盖日志/指标等旁路，不能只验证
  KV 响应。

  Prometheus adapter 现统一用 replacement rune 规范化所有 global/local label value
  为合法 UTF-8，业务调用点无需各自猜测哪些字段可能来自用户键；label name 仍是受信
  配置。回归测试覆盖 counter、gauge、histogram 三类 Emit 和最终 Gather 标签。
  A162 重启测试以 A163 镜像再次执行约 22.7 秒，普通/二进制历史与租约状态全部恢复；
  滚动后的三个 KubeBrain Pod 均 Ready、restartCount=0，日志无 panic/invalid UTF-8。
- **Storage A164 internal-row object scan filter（2026-07-18）**：A162 首轮真实重启
  暴露 count-index 全租户重建会把 event log、event-log watermark 和通用 internal
  KV 当作 object-MVCC key 解码。结果因解码失败后跳过而未产生错误计数，但每次启动
  会产生大量 `unmarshal object key` 日志和无效格式化，既增加启动 CPU/I/O，也会
  淹没真正的未知物理 key 损坏。

  scanner 现由 keyspace 注入统一的 `IsInternalStorageKey` 分类器；Range、RangeStream、
  CompactKeys 和通用分区 scan 的 worker 对已知三类内部行仅作 V(4) 跳过，未知畸形
  key 仍保留 error 日志。单测在同一物理范围混入一个合法对象和三类内部行，固定对象
  结果、分类次数及未知畸形 key 不得误分类；focused race 和主模块全量测试通过。
  A164 镜像在真实 3 PD/3 TiKV、三副本 KubeBrain 上启用 count index 后，从 TiKV
  重建 5,645/5,651 个 live key 分别耗时约 462/420ms，日志无 object-key 解码错误；
  最终重启持久化回归约 22.0 秒通过，三个 Pod Ready、restartCount=0。
- **Deploy A165 fully-qualified peer DNS（2026-07-18）**：dev、production plain/TLS
  StatefulSet 的 advertise identity、静态 initial-cluster 和 peer TLS server name
  统一改为 `.svc.cluster.local`，避免运行时 resolver 对 `.svc` 短名是否继续应用搜索域
  的差异；TLS smoke 证书同步加入完整 service FQDN SAN，manifest 测试固定 plain/TLS
  和 dev 三套配置。

  Pod 内对三个完整 ordinal FQDN 的解析验证通过，真实滚动重启及二进制历史恢复约
  22.0 秒通过。删除旧 ordinal 到新 Pod EndpointSlice 建立之间仍存在短暂 NXDOMAIN，
  这是 StatefulSet/EndpointSlice 生命周期窗口而非搜索域问题；当前 clientv3 和内部
  leader 连接会重试并恢复。后续可通过故障预算测试量化窗口，但不得把完整 FQDN
  误记为消除 Pod 替换期间的 DNS 不可用。
- **Test A166 destructive binary-range isolation（2026-07-18）**：在真实集群运行
  全套双端差分时，A162 重启测试遗留的 `0xfe` 前缀键被 binary mutation 的
  `[fe,ff)` DeleteRange 当作测试数据删除；旧 cleanup 还分别使用 `ff`/`00`
  FromKey，测试失败时可能清理共享 endpoint 上不属于测试的任意业务键。该失败不是
  KubeBrain 范围语义差异，而是兼容测试本身缺少隔离保护。

  binary key/mutation 差分现于写入前用 KeysOnly+Limit=1 检查所有将读取或删除的全局
  二进制范围，发现任意既有键即明确要求 disposable endpoint 并 fail-fast；cleanup
  只逐项删除测试声明的 exact key，不再执行开放范围删除。清理由 `t.Cleanup` 在写入
  前注册，部分写入或后续断言失败也不会扩大影响。干净范围上的两项双端差分通过，
  随后参考 etcd `d947b2086` 与真实 TiKV-backed KubeBrain 的完整兼容模块 176.9 秒
  通过；仓库全量测试通过。该项仅修改测试工具，无需重建数据面镜像。
- **Txn A167 nested operation budget differential（2026-07-18）**：逐行对照
  `/root/etcd/server/etcdserver/api/v3rpc/key.go:checkTxnRequest`，确认 txn 的
  `--max-txn-ops` 不是简单递归总数：每层先以 compare/success/failure 三者最大长度
  扣减预算，普通 Range/Put/Delete 不继续收费，只有 nested Txn 使用剩余预算；未选
  success/failure 分支也必须完整校验。

  双端矩阵新增顶层 128/129、外层 126+一个 nested child 恰好通过、127+child 超限、
  128 compare 加一个普通 Range 仍通过，以及未选 failure 分支 nested 超限共六种边界，
  结构化比较 gRPC code/message、是否返回响应和 Succeeded。参考 etcd `d947b2086`
  与真实 TiKV-backed KubeBrain 全部一致，focused race 和仓库全量测试通过。该轮未发现
  数据面实现差异，仅补强生产配额契约的黑盒证据，因此无需重建镜像。
- **Deploy A168 count-index declarative defaults（2026-07-18）**：运行集群已手工启用
  count index 并用于 A164 重启验证，但 dev、production plain/TLS manifest 未固化该
  参数，重建实例会退回全表 Count 扫描；生产文档又明确要求大规模 List/count 开启
  A-index，形成声明式配置漂移。三份清单现统一配置
  `--enable-count-index=true`、`--count-index-max-keys=5000000` 和
  `--enable-storage-metrics=true`，manifest 测试固定三项。

  5M cap 与代码默认一致，适配模板的 4Gi memory limit；超过上限时索引释放并回退扫描，
  不以 OOM 换性能。`storage-gc-lifetime=10m` 已由程序默认生效，通用模板不重复配置；
  auto-compaction retention 仍按 Kubernetes/Cilium 等租户策略由控制面选择，不能使用
  一个通用硬编码值。真实三副本滚动后 leader 从 TiKV 重建 5,865 个 live key，约
  694ms、ready=true；CountOnly 与全量 Range 均为 5,865，`storage_iter_*` 指标已在
  `/metrics` 暴露，三个 Pod Ready 且零重启。清单测试和仓库全量测试通过；该项仅改变
  部署参数，继续使用已验证的 A164 数据面镜像。
- **Observability A169 count-index degradation alerts（2026-07-18）**：A168 默认启用
  5M cap 后，超过上限会正确释放索引并回退 TiKV 全扫，但 production monitoring
  没有告警，可能把突发的 List/count 读放大静默留到客户端超时才发现。现新增
  `KubeBrainCountIndexOverflowed`：任一
  `count_index_overflowed{namespace="kubebrain-system"} > 0` 持续 1m 告警；新增
  `KubeBrainCountIndexRebuildFailures`：10m 内
  `count_index_rebuild_err` 增量非零立即告警。

  两项均为 warning，因为数据正确性仍由 scan fallback 保持；处置要求同时扩实例内存
  和 key cap，禁止只放大 cap。未使用 `count_index_keys == 0`，因为 leader-only 索引
  使两个 follower 的 keys 正常为 0。真实三副本 `/metrics` 验证 leader
  `keys=5865, overflowed=0`、followers `keys=0, overflowed=0`，证明该聚合形状不会把
  follower 误报为故障。manifest 测试固定完整 PromQL、for 和 severity，仓库全量测试
  通过；当前 kind 未安装 Prometheus Operator/promtool，规则加载验证仍由目标监控环境
  的发布流水线负责。
- **Observability A170 expected etcd error alert filtering（2026-07-18）**：production
  `KubeBrainGrpcErrors` 原先匹配所有 `grpc_code!="OK"`，会把 NotFound、
  InvalidArgument、OutOfRange、Unavailable、ResourceExhausted 和 Unimplemented
  等正常 etcd 控制流、调用方错误、换主、配额保护或平台替代能力持续报为服务故障。
  `KubeBrainWriteFailures` 同样只看 `success=false`，没有使用已存在的低基数
  `errclass`，会把可恢复错误升级为 critical。

  gRPC 规则现仅匹配 `Unknown|Internal|DataLoss`；write 规则仅匹配
  `errclass=~"deadline|other"`，明确排除 revision、unavailable、fenced、not_found、
  invalid 和 canceled。真实三副本 endpoint 注入不存在 Lease 的 Revoke/Put，按 etcd
  语义返回 NotFound，并在多个副本产生 `grpc_server_handled_total{grpc_code="NotFound"}`
  计数；新规则不匹配，旧规则会误报。manifest 测试固定完整 PromQL 并禁止退回
  `grpc_code!="OK"`，仓库全量测试通过。该项仅修改监控与文档，无需数据面镜像。
- **Observability A171 alert metric contract（2026-07-18）**：审计 production
  PrometheusRule 引用的全部指标，当前名称均能映射到 KubeBrain emitter，或明确来自
  gRPC middleware、kube-state-metrics 和 Prometheus `up`。新增 CI 契约测试：解析
  monitoring manifest 中每条告警的 PromQL 指标 token，扫描 `pkg` 下 Go emitter，
  将点号归一化为 Prometheus 下划线名称，并补齐 histogram 的 `_bucket`、`_count`
  和 `_sum` 派生序列；仅显式放行
  `grpc_server_handled_total`、`grpc_server_handling_seconds_bucket`、
  `kube_statefulset_status_replicas_ready` 和 `up` 四类外部指标。

  部分 counter 采用首次 emit 时注册，未触发对应路径的单次 `/metrics` 抓取不会出现
  该序列，因此实时抓取只能验证当前运行路径，不能代替静态 emitter 契约。production
  定向测试和仓库全量测试均通过；该项仅新增测试与文档，无需重建数据面镜像。
- **Observability A172 PD/TiKV health coverage（2026-07-18）**：独立实例原先只有
  KubeBrain ServiceMonitor 和告警，PD/TiKV 即使丢 quorum、无 leader 或 Region 无
  leader，也只能等客户端错误间接暴露。production 现为 `kb` 实例增加两个 headless
  metrics Service：PD 2379 和 TiKV 独立 20180 端口，并由跨 namespace ServiceMonitor
  逐 Pod 抓取；selector 固定 instance/component/part-of，不会把同 namespace 的恢复
  集群混入生产实例。

  新增 critical 告警覆盖 PD/TiKV 任一可抓取副本少于 3、PD
  `etcd_server_is_leader` 总和不为 1，以及
  `tikv_raftstore_leader_missing > 0`。真实 3 PD/3 TiKV 集群的两个 Service 均生成
  3 个 Ready EndpointSlice 地址；逐 PD 抓取 leader 值为 `0/1/0`，TiKV
  `leader_missing=0`。manifest 测试固定 Service selector/端口、ServiceMonitor
  namespace/selector 和完整 PromQL，指标契约显式登记两个后端外部序列，仓库全量测试
  通过。当前 kind 未安装 Prometheus Operator CRD，不能在该环境验证规则加载；目标
  监控栈仍须执行 CRD admission/promtool 门禁。该项只改变部署监控，无需重建数据面镜像。
- **Observability A173 storage capacity guard（2026-07-18）**：Maintenance
  `DbSize`/Alarm 采用 TiKV/PD 平台替代后，production 仍没有对应 PVC 容量告警，磁盘
  耗尽只能由后端写失败间接暴露。现对固定三副本 `kb` 实例的
  `pd-kb-pd-[0-2]`、`tikv-kb-tikv-[0-2]` 增加两层规则：6 个 PVC 中
  `kubelet_volume_stats_capacity_bytes` 少于 6 持续 15m 报 warning，显式识别不支持
  volume stats 的 CSI 驱动；任一 PVC 的 available/capacity 低于 15% 持续 10m 报
  critical。

  真实集群确认 3 个 PD PVC 和 3 个 TiKV PVC 的名称、instance/component 标签及容量；
  kind local-path 驱动不暴露 kubelet volume stats，因此在目标监控栈中会按设计触发
  telemetry-missing，而不是静默跳过低容量保护。manifest 测试固定完整 PromQL、
  severity/for，并将两个 kubelet 序列纳入外部指标契约；Prometheus 3.5
  `promtool check rules` 成功解析全部 18 条规则，仓库全量测试和真实 endpoint health
  通过。该项仅修改监控与测试，无需重建数据面镜像。
- **Observability A174 resource saturation guard（2026-07-18）**：production 已为
  KubeBrain/PD/TiKV 配置 CPU、内存 request/limit，但原监控无法在 OOMKill 或延迟尖峰
  前识别资源饱和。现对 3 个 KubeBrain、3 个 PD、3 个 TiKV 主集群容器增加三项规则：
  cAdvisor working-set 或 kube-state-metrics memory limit 序列不足 9 个持续 15m 报
  telemetry warning；working-set/limit 超过 90% 持续 10m 报 critical；5m CPU CFS
  throttled periods 比例超过 25% 持续 15m 报 warning。

  TiKV/PD selector 使用 `kb-(pd|tikv)-[0-2]`，不会把 `kb-restore-*` 混入售卖实例。
  真实 kubelet cAdvisor 验证 container/namespace/pod/image 标签，并精确匹配 6 条主存储
  集群 working-set 序列；当前 dev 工作负载没有 limit，不能用于验证 production quota
  比例，这是清单差异而非规则缺口。manifest 测试固定完整 PromQL、severity/for 和全部
  外部指标名；Prometheus 3.5 成功解析 21 条规则，仓库全量测试、TidbCluster Ready 与
  endpoint health 均通过。该项只修改监控与测试，无需重建数据面镜像。
- **Observability A175 data-plane network guard（2026-07-18）**：独立实例的
  KubeBrain、PD、TiKV 网络此前没有统一故障指标或用量基础。现对 9 个主数据面 Pod 的
  `eth0` 增加三项 warning：RX 或 TX byte counter 少于 9 条持续 15m 报 telemetry
  missing；10m 内 RX+TX interface errors 非零；10m 内 RX+TX packet drops 非零。
  selector 固定 `kubebrain-[0-2]` 和 `kb-(pd|tikv)-[0-2]`，排除恢复集群。

  真实 kubelet cAdvisor 分别精确匹配 9 条 RX 和 9 条 TX byte 序列，当前目标接口累计
  error/drop 均为 0；这些 byte counter 也提供按实例聚合网络用量的原始数据，但不等同
  于控制面已完成账单归集。manifest 测试固定完整 PromQL、severity/for 和六个网络外部
  指标；Prometheus 3.5 成功解析全部 24 条规则，仓库全量测试、TidbCluster Ready 和
  endpoint health 通过。该项只修改监控与测试，无需重建数据面镜像。
- **Compatibility A176 automated full differential gate（2026-07-18）**：此前各组
  参考 etcd 差分虽持续运行，但需要人工启动 `/root/etcd/bin/etcd`、选择端口、设置环境
  变量和清理 data-dir，容易在 CI 中漏跑或复用污染状态。新增可执行入口
  `hack/etcd-client-compat/run-differential.sh`：要求显式
  `KUBEBRAIN_ETCD_ENDPOINT`，拒绝已占用的参考端口，创建临时单成员 etcd，等待
  `/health`，以 `-parallel=1` 运行全部 `Differential` 测试，并用 EXIT trap 停止进程、
  打印失败日志及删除临时数据。

  真实 TiKV/PD-backed endpoint `127.0.0.1:4379` 上端到端执行成功，62.662s 内通过
  Alarm、binary key/mutation、client version、Compact、DeleteRange、HashKV、Lease、
  MemberList、Put、Range/RangeStream、Txn 和 Watch 的全部通用双端组；脚本退出后
  12379 端口释放且临时目录清理。Auth/JWT 差分按设计跳过，因为它们要求独立、可销毁且
  配置不同的认证实例，仍由专用 auth/JWT 入口负责，不能据此声称认证差分已运行。
  `bash -n`、仓库全量测试和 endpoint health 通过；本机没有 shellcheck。
- **Compatibility A177 destructive differential isolation（2026-07-18）**：复审 A176
  runner 发现完整套件包含 Compact；即使测试 key 均隔离，Compact 仍会推进目标实例的
  全局 compact revision，可能删除其他租户/测试的历史，因此“显式 endpoint”不足以防止
  误指生产。runner 现默认 fail-closed，必须同时设置
  `ALLOW_DESTRUCTIVE_DIFFERENTIAL=true`，并在创建参考 data-dir 前用
  `etcdctl endpoint health` 验证目标；mTLS 继续通过标准
  `ETCDCTL_CACERT`/`ETCDCTL_CERT`/`ETCDCTL_KEY` 环境传入。

  未设置确认变量时稳定拒绝且说明 Compact 风险；确认后指向不可达 endpoint 时在参考
  实例启动前失败且不产生临时目录；确认后指向 disposable 真实 TiKV/PD endpoint 的完整
  差分 52.183s 通过，退出后 12379 端口和临时目录均释放。`bash -n`、仓库全量测试、
  TidbCluster Ready 与 endpoint health 通过。控制面/CI 必须只给一次性兼容测试实例设置
  该变量，禁止在共享或生产实例上运行。
- **Operations A178 logical backup capacity and freshness metrics（2026-07-18）**：
  逻辑备份此前只有 artifact 完整性，没有可供控制面判断容量和 RPO 的稳定指标。export
  现可通过 `METRICS_OUTPUT` 在 artifact 原子提交后发布 Prometheus textfile，包含最近
  成功时间、artifact bytes、record/lease count 和 snapshot revision，并以
  `BACKUP_INSTANCE` 区分实例。指标同样使用临时文件、`fsync`、rename 和目录
  `fsync`；失败导出不会刷新上一次成功时间，避免把失败尝试误报成可恢复备份。

  production rule 对默认实例的成功指标缺失 1 小时 warning，超过 25 小时未成功且
  持续 15 分钟 critical。单元测试固定 label 转义、权限、全部 gauge、临时文件清理和
  错误输入不覆盖旧成功状态；manifest 测试固定 PromQL、severity/for 和指标存在性。
  真实 TiKV/PD endpoint 对隔离前缀导出 1 条记录：v2 artifact 完整性、revision 和
  SHA-256 校验通过，textfile 报告 370 bytes、1 record、0 lease，测试 key 随后清理。
  该能力提供单实例原始容量/新鲜度；跨实例保留策略、对象存储占用和计费聚合仍是 P1。
- **Operations A179 instance metering recording rules（2026-07-18）**：production
  PrometheusRule 新增 1 分钟实例级聚合，统一输出
  `dbaas_instance="kubebrain"`：数据面 CPU cores、working-set memory、RX/TX
  bytes/s、PD/TiKV PVC provisioned/used bytes，以及最近逻辑备份 artifact bytes
  和 age。容器选择器严格限定 3 个 KubeBrain 与独立 `kb` 集群的 3 PD/3 TiKV，
  PVC 选择器严格限定对应 6 个卷，避免同 namespace 其他实例串账；used bytes 与
  backup age 对短暂抓取/时钟偏差执行 `clamp_min(..., 0)`。

  manifest 测试固定 8 个 record 名、完整 PromQL、1 分钟 interval 和实例标签。
  Prometheus 3.5 `promtool check rules` 成功解析全部 34 条规则；合成规则测试分别输入
  KubeBrain/PD/TiKV counter、memory、network、PVC 和 backup 序列，验证 8 个实际
  求值及负 storage/age 钳制为 0。该层已提供规范化单实例计量输入；控制面缺测策略、
  不可变采样留存、跨周期积分、价格版本、对象存储保留成本和审计对账仍未完成。
- **Operations A180 actionable etcdctl platform errors（2026-07-18）**：对照
  `/root/etcd/etcdctl/ctlv3/ctl.go`、各 command 和 etcd Maintenance/Cluster RPC，
  建立 `docs/etcdctl_compatibility_cn.md`。平台替代 RPC 继续使用标准
  `codes.Unimplemented`，但错误现明确下一步：member mutation 转 DBaaS 扩缩/重配置，
  snapshot 转 logical backup/restore，MoveLeader 转 rollout/failover，Downgrade 转
  versioned rollout/rollback，alarm mutation 转 PD/TiKV 告警与修复。错误常量集中定义，
  同时保留 etcd 的授权顺序，非 root 不会先看到平台管理信息。

  server 单测逐字固定 code/message 和 auth 顺序；官方 client/v3 live test 覆盖
  AlarmDisarm、SnapshotWithVersion、MoveLeader、Downgrade 及四个 member mutation。
  新镜像 `kubebrain:a180-platform-guidance` 在真实三副本数据面滚动完成后，
  `/root/etcd/bin/etcdctl` 逐命令验证上述平台操作均非零退出并显示替代路径；
  endpoint health/status、member list、alarm list 和 defrag 仍成功，status 正常显示
  3.7.0、revision/term 和 1 B sentinel。无 alarm 时 `alarm disarm` 由客户端 list 后
  直接成功，不发送 mutation；direct mutation 的提示由 live client test 固定。
- **Compatibility A181 explicit public RPC surface guard（2026-07-18）**：逐项枚举
  `/root/etcd/api/etcdserverpb/rpc.proto` 和当前 `go.etcd.io/etcd/api/v3 v3.7.0`
  生成的六个 gRPC `ServiceDesc`，确认 42 个公共 RPC 均有 KubeBrain 显式方法，没有
  依赖嵌入 `Unimplemented*Server` 才“实现”的静默缺口。KV、Watch、Cluster、
  Maintenance、Auth 归属 `RPCServer`，五个 Lease RPC 归属独立 `leaseManager`。
  proto 中的 `LeaseCheckpointRequest` 只被
  `/root/etcd/api/etcdserverpb/raft_internal.proto` 引用，是 etcd 内部 Raft apply
  消息而非公开 Lease Service RPC；KubeBrain 已用 TiKV 持久 remaining-TTL checkpoint
  实现等价 failover/restart 边界，不能为它伪造一个公开 endpoint。

  新增 AST + descriptor guard：测试从生产 `.go` 文件收集 receiver 上显式声明的方法，
  对每个 unary/stream descriptor 要求正确 owner，并固定当前总数 42。未来升级 etcd
  API 后，只要增加 RPC、移动 owner 或仅由 forward-compat shim 接住，测试都会要求先
  分类并实现/明确拒绝。针对性测试、race 和 vet 通过；首次人工预估 43 被权威 descriptor
  校正为 42，六个 service 的逐方法检查在总数断言前已全部通过。
- **Operations A182 executable instance release gate（2026-07-18）**：新增
  `hack/production/validate-instance-ready.sh`，把控制面创建、扩缩和升级的成功条件从
  单一 rollout 提升为完整数据面证据。`EXPECTED_IMAGE` 与 `ENDPOINT` 必填且 fail
  closed；先复用 TidbCluster convergence gate，要求 Ready condition 和 PD/TiKV
  StatefulSet generation/ready/updated/revision 收敛，再核对期望 PD/TiKV 数量；随后
  核对 KubeBrain StatefulSet observed generation、期望 replicas、ready/updated、
  current/update revision 和精确 image，最后执行官方 `etcdctl endpoint health`
  线性化 proposal。TLS/mTLS 沿用标准 `ETCDCTL_CACERT/CERT/KEY`。

  mock command 测试覆盖收敛发布、错误镜像、stale rollout、错误存储拓扑、endpoint
  不健康和缺少必填输入；shell syntax、Go test 和 vet 通过。真实
  `kind-kubebrain-dbaas` 上以当前 `kubebrain:a180-platform-guidance`、3 KubeBrain、
  3 PD、3 TiKV 和 `127.0.0.1:4379` 执行，TidbCluster convergence、proposal 和最终
  release gate 全部通过。生产要求 digest；脚本只做精确匹配，不替控制面判断 tag
  可变性。备份、恢复、证书轮换和销毁的幂等状态机/回滚证据仍是 P1。
- **Operations A183 protected backup completion gate（2026-07-18）**：逻辑 v2
  artifact header 新增 `created_at_unix`，记录 snapshot 导出开始时间，并与 format、
  prefix、revision 和 records 一起进入 footer SHA-256。`logical-status` 新增
  `EXPECTED_PREFIX`、`MIN_RECORDS` 和 `MAX_AGE_SECONDS`，先完成全文件复制/摘要验证，
  再执行精确 prefix、非空/容量下限和 RPO freshness gate；超过 5 分钟的未来时间也
  fail closed。旧 v1 和没有时间戳的早期 v2 保持可读/可恢复，但不能在要求 freshness
  时靠文件 mtime 冒充新备份。

  单元测试覆盖新 writer/status 时间戳、旧 v1/v2 兼容、正确 completion contract、错
  prefix、记录不足、缺时间戳、过期、未来时间和非法门禁参数；backup 相关测试和 race
  通过。真实 TiKV/PD endpoint 导出隔离 prefix 的 1 record v2 artifact，status 返回
  revision `467764733078405126`、受保护时间戳和 SHA-256；正确 prefix/min=1/age=60
  通过，错 prefix 非零退出，等待后 age=3s/max=1s 非零退出。手工只把 header 时间戳
  加 1 秒后，status 在年龄判断前以 SHA-256 mismatch 拒绝；隔离测试 key 已清理。
  额外把含新 header 的 2-record artifact 恢复到隔离 prefix，count 和逐值 verify
  通过，源/目标均清理。这关闭备份 artifact 的完成/RPO 证据；对象存储上传幂等和
  保留删除由 A188 关闭，恢复流量切换状态机仍是 P1。
- **Operations A184 immutable restore verification receipt（2026-07-18）**：
  `logical-verify` 新增可选 `RECEIPT_OUTPUT`，仅在 artifact 完整性、全部目标 key/value、
  permanent/lease 绑定映射和目标 lease 正 TTL 验证完成后发布
  `kubebrain.restore-verification.v1`。receipt 绑定 artifact format/SHA-256/snapshot
  revision/创建时间、source/target prefix、record count、artifact/target lease count
  和 `verified_at_unix`，不写 endpoint/证书。启用 receipt 且 rewrite 时，
  `REWRITE_FROM` 必须精确等于 artifact prefix 且 `REWRITE_TO` 非空，避免验证子树后
  生成“整份恢复完成”的含糊证据。

  receipt 以 0600 同目录临时文件写入、file `fsync` 后原子 hard-link 到最终路径并同步
  目录；已存在的 operation receipt 不能覆盖。单元测试覆盖完整 JSON/权限/临时文件
  清理、不完整输入不替换旧证据、第二次有效写也不能替换，以及 target prefix 绑定；
  receipt/verify race 和 vet 通过。真实 TiKV/PD 隔离演练导出并恢复 2 records（1
  permanent、1 leased），receipt 的 artifact hash/revision、source/target、2 records、
  1 artifact lease/1 verified target lease 全部核对；篡改目标 permanent value 后
  verify 非零退出且未发布第二 receipt，源/目标 lease 已 revoke、prefix 已清理。
  receipt 是验证时点证据；对象存储不可变留存由 A188 关闭，流量切换 fencing 和恢复后
  持续审计仍是 P1。
- **Operations A189 UID-fenced restore traffic cutover（2026-07-18）**：新增
  `hack/production/switch-restore-traffic.sh`，将恢复流量发布收敛为
  `prepare -> cutover -> verify -> complete`，并提供 complete 前的 `rollback`。
  prepare 校验并冻结 A184 restore receipt、Service UID/resourceVersion、精确的
  name/instance selector，以及源/目标全部 Ready Pod 的 name/UID/restart count。
  cutover 使用含 UID、resourceVersion 和旧 selector 三个 `test` 的 JSON Patch 原子
  CAS；selector 改变后不以 patch 成功作为发布成功，而要求受同一 Service UID 控制的
  EndpointSlice 全部 Ready/Serving/非 Terminating，targetRef UID 集精确等于冻结目标
  Pod UID 集。verify 与 complete 均从公开 Service endpoint 对完整 logical artifact
  重做 key/value 和 lease 校验，并要求稳定 receipt 字段与 prepare 凭据一致；complete
  才发布不可覆盖的 `kubebrain.restore-cutover.receipt.v1`。rollback 以同样 CAS 切回
  冻结源 Pod UID 集；complete 后禁止 rollback，rollback 后禁止 complete。

  mock 故障矩阵覆盖幂等生命周期、回滚、阶段越级、初始 selector 错误、Pod UID 替换、
  Service UID 替换、resourceVersion CAS 冲突、外来 EndpointSlice UID、公开数据校验
  失败和缺 verify 完成。真实 kind 独立 namespace 使用两组各 2 个 Pod 与真实
  EndpointSlice controller 完成 source→target→source；首次夹具携带额外 selector 时
  EndpointSlice 为空，门禁超时拒绝发布，修正为精确 selector 后同一 operation 安全
  重试通过。A190 又为 receipt 增加 cutover state SHA-256，将冻结 Pod UID 行绑定到
  完成证据。控制面 API/队列编排仍是 P1。
- **Operations A190 post-restore continuous data audit（2026-07-18）**：新增
  `hack/production/cmd/etcd-audit-probe` 与
  `hack/production/audit-restored-instance.sh`。官方 etcd 可比较各成员独立 backend 的
  `HashKV` 并触发 CORRUPT alarm；KubeBrain 三个网关共享 TiKV MVCC，成员 hash 比较没有
  独立副本语义。因此 A190 在持续窗口中审计实际服务路径：每个样本经公开 endpoint 完成
  lease grant、`createRevision=0` 条件 Put、线性 Get 的 value/lease 核对、value 条件
  Delete、删除确认和 revoke；随机 key 位于专用审计 prefix，lease 限制故障残留。

  每个探针前后都核对 A189 state SHA-256、Service UID/精确 target selector、目标 Pod
  name/UID/restart/Ready 集及 EndpointSlice owner/targetRef UID 集。窗口使用单调时钟，
  同时要求最短 duration、最少 samples 和跨样本 revision 单调不降；任一步失败不发布
  receipt。完成后以 0600、file/directory fsync 和不可覆盖 hard-link 签发
  `kubebrain.post-restore-audit.receipt.v1`；重试已有 receipt 仍执行在线拓扑与事务探针。
  mock 覆盖 receipt/state 篡改、Service/Pod/EndpointSlice 漂移、探针失败、畸形证据和
  revision 回退。真实三副本 KubeBrain/TiKV endpoint 探针写 revision
  `467764733078405147`、删 revision `467764733078405148` 且 prefix 零残留；随后真实
  Kubernetes Service/EndpointSlice controller 与同一数据面完成 2 秒、3 样本窗口，
  首末 delete revision 为 `467764733078405150/467764733078405154`，receipt 发布且
  prefix 零残留。多租户审计聚合仍是 P1。
- **Operations A191 persistent operation API and worker fencing（2026-07-18）**：
  新增 namespaced `KubeBrainOperation.dbaas.kubebrain.io/v1alpha1` CRD、
  `hack/production/internal/operationqueue` 和 `operationctl`。spec 绑定 operation ID、
  instance、类型、完整参数 SHA-256 与 maxAttempts，并由 CEL admission 保证不可变；
  status 使用 Kubernetes resourceVersion CAS 认领。Pending 或过期 Running 可 claim，
  每次递增 attempt；owner+attempt 是 fencing token，旧 worker 不能 heartbeat/retry/
  finish。heartbeat 续租，retry 清 owner/lease 后保留 attempt，达到 maxAttempts 的任务
  被扫描为 Failed；Succeeded/Failed 终态不可变。同 owner/attempt/receipt finish 可在
  响应丢失后幂等重试，不同终态证据拒绝覆盖。

  CRD CEL 还要求 attempt 不下降/不超过上限、owner 变更必须提高 attempt 或显式 requeue、
  Running 必须有 owner/attempt/lease、Pending 不能残留 owner/lease、终态必须清 lease
  并记录完成时间、Succeeded 必须有 receipt SHA-256。worker Role 只允许读取 operation
  与更新 status，不允许 create/delete/spec 写入。真实 kind API Server 上两 worker 并发
  claim 结果为 `0/3`（仅一个成功）；2 秒 lease 过期后 worker-c 以 attempt 2 接管，旧
  worker heartbeat 非零退出；重复 finish 成功，直接复活终态、修改 spec 和不增 attempt
  换 owner 均被 admission 拒绝。ServiceAccount impersonation 验证
  get/status-update=`yes/yes`、create/delete=`no/no`。

  `run-post-restore-audit-operation.sh` 首个接入 A190：只 claim PostRestoreAudit，核对
  参数文件 digest，运行期间 heartbeat，fencing 时终止子进程，失败 requeue，成功将
  A190 receipt SHA-256 提交终态。真实 CRD→claim→A190 2 秒/3 样本→finish 链路以
  attempt 1 完成，status receipt digest
  `a1a8333123fcb2912603d7e416124c5f9e5f2eb393d62776a21ae7686c3ad245`，
  探针 prefix 零残留。A192-A195 已接入 Backup/RestoreCutover/CertificateRotation/
  Destroy executor；A196/A197 已完成 Kubernetes 原生提交身份和跨实例公平调度，
  外部管理 API 认证授权及审计聚合仍是 P1。
- **Operations A192 protected Backup operation executor（2026-07-18）**：新增
  `hack/production/run-backup-operation.sh`，只 claim `Backup` operation 并核对完整参数
  JSON SHA-256。参数固定数据 endpoint/prefix、operation 专属 artifact/receipt 路径、
  batch、Object Store ID、bucket/key、retention mode、绝对 retain-until 和 completion
  gate。首次运行 logical v2 export；崩溃重试发现 artifact 已存在时禁止覆盖，先重做
  exact prefix/min records/freshness 校验，再调用 A188 条件上传与 exact version 远端
  下载/retention 复核。导出/上传期间持续 heartbeat；失败 requeue 并消耗 attempt，
  heartbeat fencing 会终止子进程；Succeeded 绑定 object receipt SHA-256。AWS 与 etcd
  凭据只由 worker 环境提供，不写参数 CR。

  mock 覆盖首次导出、已有 artifact 安全复用、上传失败 requeue、参数 digest 漂移和
  heartbeat fencing。真实 CRD→Backup executor→三副本 KubeBrain/TiKV→MinIO Object
  Lock 链路导出 `/kubebrain-a192-backup` 1 record，snapshot revision
  `467764733078405161`、artifact digest
  `c6cdd323b0fcc412ac01d4a6978be0604e935a772d7201362ef535607c039e05`；
  exact version `4368f8b1-dec6-4d2e-bad8-ff9f08e72824` 远端复核后，operation attempt 1
  进入 Succeeded，status receipt digest
  `8d7e1eeff89f3039a7af47fc96bf7e7efe7730069f3a99992e0ebf38a349af97`。
  COMPLIANCE 保留到期后按 exact version 删除并签发 deletion receipt，源 prefix 计数 0。
- **Operations A193 resumable RestoreCutover executor（2026-07-18）**：新增
  `hack/production/run-restore-cutover-operation.sh`，绑定 A184 restore receipt、logical
  artifact、A189 state/receipt、Service 与源/目标 instance 等完整参数摘要，按
  prepare→cutover→verify→complete 驱动并逐阶段 heartbeat。prepare 失败 retry；cutover
  开始后任一阶段失败都尝试 rollback，再以 rollback 成功/失败明文写 Failed，禁止把可能
  已改变 selector 的操作普通重试。fencing 会终止子进程且不再操作流量。

  接管恢复按不可变证据选择起点：state-only 从 cutover、cutover marker 从 verify、
  receipt 从 complete 在线复检继续，rollback marker 直接终态失败。同步修复 A189 的部分
  切换窗口：Service patch 可能已提交但 EndpointSlice 等待失败，此时没有 cutover marker；
  rollback 现只要求 prepare state，再按当前 selector CAS 切回并验证冻结源 Pod UID 集。
  测试覆盖全成功、prepare retry、cutover/verify/complete 自动 rollback、rollback 失败、
  参数漂移、heartbeat fencing、四类接管证据及 patch-before-marker 回滚。

  真实 CRD→executor→A189 使用两组各 2 个真实 Pod 与 Service/EndpointSlice controller，
  公开三副本 KubeBrain endpoint 对 1-record artifact 做 verify/complete 两次逐值复核；
  artifact revision `467764733078405163`、digest
  `60c975348322ac8cb2f4f22502cd0fab167fb9af66c27027653948ae27bdbee3`。
  Service 最终 selector=`target`，A189 receipt 的 endpoint UID/data flags 均为 true，
  operation attempt 1 Succeeded，status receipt digest
  `bd762c609c849ad2fcf6f66cfe0955383c7f78fddc7f07d631fe177195a1bdf0`；测试 prefix 与
  namespace 已清理。A194/A195 已接入 CertificateRotation/Destroy。
- **Operations A194 resumable CertificateRotation executor（2026-07-18）**：新增
  `hack/production/run-certificate-rotation-operation.sh`。operation 参数除旧/新/overlap
  CA、client cert/key 路径外还绑定每个文件 SHA-256，发布前逐文件复算；固定路径内容漂移
  会在任何 hook 前拒绝并 requeue。双 CA 与最终 Secret 发布由 worker 镜像配置的受控、
  可执行、幂等 hook 完成，不允许 CR 注入命令。顺序为 begin→publish-overlap→overlap→
  publish-final→complete；state、overlap marker、receipt 接管分别从对应安全阶段继续。
  任一步失败 requeue，heartbeat fencing 终止当前子进程且不执行后续发布。

  mock 覆盖完整生命周期、五步骤逐点失败、三类证据恢复、参数/credential 内容漂移与
  heartbeat fencing。真实 operation 使用独立参考 etcd mTLS endpoint 和当前 3 个
  KubeBrain Pod 的 UID/restart/Ready 快照：初始只信旧 CA，overlap hook 切到双 CA，
  final hook 切到仅新 CA；A185 complete 实际观察旧 client 非零退出、新 client 正向
  proposal 成功。operation attempt 1 Succeeded，receipt digest
  `a1f14e461bcce9033bf366e92cf6a396f30aee2174698a700346e5ad2a95313c`，
  old/new cert fingerprints 为
  `d1722aaf6be371394a7997b6bba0dc1307ca55a47ebaef35d31e7387933d1a71` /
  `ed961e379fe806bfcbb82e148951c05fd55d28c0e408d2d6a495ceb214ae6f69`，
  Pods unchanged 与 old rejected 均为 true，隔离 endpoint/namespace 已清理。

  本轮真实 hook 首次还暴露 Bash `kill -0` 会把未 wait zombie 视作存活并无限 heartbeat；
  Backup、PostRestoreAudit、RestoreCutover、CertificateRotation 四个 executor 已统一改为
  主进程 wait 工作子进程、独立 heartbeat 续租，结束后回收 heartbeat，fencing 时由
  heartbeat 杀工作进程。A185 同时补显式 KUBECONFIG_PATH。
- **Operations A195 resumable Destroy executor（2026-07-18）**：新增
  `hack/production/run-destroy-operation.sh`，只 claim `Destroy` operation，并把 A187
  state/backup/receipt 路径、备份文件 SHA-256、逻辑 prefix/freshness/record gate、
  KubeBrain/TidbCluster 身份、期望 PVC 数量和超时参数完整绑定到 operation 参数摘要。
  worker 在任何删除前复算参数文件与备份字节摘要，并要求确认令牌精确等于
  `destroy:<instance>:<operation_id>`；错误确认直接进入 Failed，备份字节漂移 requeue，
  两者都不会启动 prepare。

  执行器按 prepare→quiesce→destroy→complete 驱动 A187，每阶段由独立 heartbeat
  续租；fencing 会终止当前子进程且不再修改 operation 状态。接管时根据不可变 state、
  quiesced、destroyed marker 或 receipt 从最早安全阶段恢复；A187 的 UID precondition
  与 NotFound 幂等语义保证部分删除后可继续，成功状态绑定最终 destroy receipt
  SHA-256。mock 覆盖完整生命周期、四阶段逐点失败、四类证据恢复、错误确认、备份漂移
  和 heartbeat fencing。

  真实 `kind-kubebrain-dbaas` 演练提交 operation `a195-destroy-1`，从
  `/dbaas/a195-destroy-proof/` 导出 1-record artifact（revision
  `467764733078405165`，artifact 内部 digest
  `3c80d5a71ef90eeba3d3d0de180c0e26f62757a4d2989f0761a1f882569d1fdf`），对独立
  KubeBrain namespace、暂停 TidbCluster 和 2 PVC 执行四阶段。12 次删除均由 API
  接受 UID precondition，固定资源、PVC 和存储 workload 残留均为 0；operation
  attempt 1 Succeeded，status receipt digest
  `b760a9860fef9326f52ae4360d8b2cb4b674208c6f3280e47770c568d9b54171`。
  隔离 namespace 与测试 key 随后清理。
- **Operations A196 least-privilege operation submitter identity（2026-07-18）**：
  新增 `deploy/production/kubebrain-operation-submitter-rbac.yaml`，将管理面提交身份与
  worker 身份分离。namespaced submitter ServiceAccount 不自动挂载 token；Role 只允许
  对 `kubebrainoperations` 执行 create/get/list/watch，不授予 update/patch/delete，
  也不授予 `kubebrainoperations/status`。因此提交者可创建并观察不可变 operation，
  但不能认领任务、伪造终态、删除审计对象或跨 namespace 提交。

  manifest 结构测试固定 ServiceAccount、Role、RoleBinding 的 namespace、subject、
  roleRef 和精确 verb/resource 集；client/server dry-run、production test、race 和 vet
  通过。真实 `kind-kubebrain-dbaas` API Server impersonation 返回本 namespace
  create/get=`yes/yes`，spec update/delete/status update/cross-namespace
  create=`no/no/no/no`；submitter 实际创建 operation 成功，实际 status patch、delete
  和跨 namespace create 均以非零退出被 RBAC 拒绝，测试 operation 已由管理员清理。
  这关闭 Kubernetes 原生提交入口的最小权限身份；外部管理 API 的 OIDC/租户授权、
  请求审批和不可变审计归档仍是 P1。
- **Operations A197 instance-fenced fair operation scheduling（2026-07-18）**：
  operation queue 不再只按 CR 创建时间全局 FIFO。每个 instance 以 SHA-256 派生固定
  `coordination.k8s.io/v1 Lease` 名称；claim 必须先用 create/update resourceVersion CAS
  获得实例 Lease，再更新 operation status。holder 绑定 operation UID、attempt 和 worker
  identity digest；heartbeat 同时续租实例 Lease 与 operation lease，requeue/finish
  释放实例 Lease。不同 operation CR 之间因此也只有一个可运行，同实例 Backup、切换、
  轮换、审计和销毁不会并发破坏状态。worker Role 仅新增 namespaced Lease
  create/get/update/delete，不获得 operation create/delete。

  候选按实例最后一次 `startedAtUnixNano` 从早到晚排序，同实例内保持 creation timestamp/
  name 稳定顺序；旧 CR 没有纳秒字段时兼容回退到 `startedAtUnix`。新纳秒字段进入 CRD
  status，避免一秒内快速任务并列后退化为名称排序。worker lease 一旦过期，即使尚未被
  新 worker 接管，旧 owner 也不能 heartbeat/requeue/finish，关闭原先只检查
  owner+attempt 而不检查 deadline 的窗口。

  单元测试覆盖同秒不同纳秒公平排序、跨实例并行、同实例串行、完成后放行下一任务、
  过期 worker 三类动作立即 fenced 和精确 Lease RBAC；production test、race、vet 与
  CRD/RBAC server-side dry-run 通过。真实 `kind-kubebrain-dbaas` 上实例 A 的两个
  Backup 与实例 B 并行 claim 为 `A1/B1`，第三个 claim 被拒绝；A1 完成后 A2 才可
  claim。同实例 D 的两个并发 CertificateRotation claim 退出码为 `0/1`，Running
  精确为 1；已有服务历史的 A 与从未服务的 C 同时排队时选择 C。纳秒状态真实持久为
  19 位 `1784390758249455229`，演练后 operation/Lease 残留均为 0。

  首次真实 claim 暴露 Lease `MicroTime` 只接受六位微秒而非 RFC3339Nano 九位格式，
  API Server 在 operation 进入 Running 前拒绝；实现改用 Kubernetes MicroTime 格式后
  重跑全部证据通过。当前公平范围是单 operation namespace；多 region/多 namespace
  管理面的全局公平仍需由上层调度器定义。
- **Operations A198 Object-Lock terminal operation audit archive（2026-07-18）**：
  新增 `kubebrain.operation-audit.v1` 规范化 artifact、`operation-audit` Kubernetes
  读取命令和 `archive-operation-audit.sh`。artifact 只接受 Succeeded/Failed 终态，
  绑定 CR namespace/name/UID/generation、不可变 operation ID/instance/type/参数
  SHA-256/maxAttempts，以及 owner/attempt/observedGeneration/start/complete/message；
  Succeeded 还必须绑定执行 receipt SHA-256，Failed 禁止伪带 receipt。JSON 使用固定
  struct 顺序、单行换行和严格 unknown/trailing/canonical 校验，以 0600、file/directory
  fsync、不可覆盖 hard-link 发布；同 operation 终态漂移不能改写本地 artifact。

  A188 objectstore module 新增专用 `archive` 动作。上传前冻结 artifact 完整字节，复核
  size/SHA-256，消除 inspect→Put 的本地文件 TOCTOU；随后使用 S3
  `PutObject If-None-Match:*`、完整 checksum、COMPLIANCE/GOVERNANCE Object Lock 和
  operation/UID/phase/execution receipt 等 metadata 条件写。返回 version ID 后重新下载
  exact version，逐字节复核并读取 retention，之后才签发不可覆盖的
  `kubebrain.object-operation-audit.receipt.v1`。已有 receipt 重试会重做远端 body 与
  retention 检查，不产生新 version；同 key 只有全部 metadata/size 一致才可恢复
  “Put 已成功、receipt 未发布”的崩溃窗口，否则拒绝替换。

  测试覆盖规范化/权限/不可覆盖、非终态、成功态缺 receipt、非法摘要、同 key 冲突、
  远端 body 损坏、retention 漂移、exact-version 幂等和不发布错误 receipt；根 module
  production test、独立 objectstore module test、两侧 race/vet 与 shell syntax 通过。
  真实 `kind-kubebrain-dbaas` 终态 operation `a198-audit-1` 归档到启用 versioning+
  Object Lock 的 MinIO，operation UID
  `5923b9fd-3ed8-4459-833c-44c5d8bf8110`、artifact digest
  `6ab9ec91367e65bfd2200d9119c22d28c636075f8a1e291dc79d11f3199c9a67`、exact version
  `b7db6435-a066-4baa-a947-d4fd145b805f`、archive receipt digest
  `57a195392c9bca4b340c8ad453382b8cb3f71e53e16b0c87b3e5d2c13ed18b43`。
  二次归档 receipt 不变；另一终态 operation 竞争同 key 非零退出且无 receipt；保留期内
  exact-version 删除被 MinIO 拒绝，到期删除后 A198 object/version 与测试 CR 残留均为
  0。A199 已补 finalizer/删除门禁；bucket inventory 对账仍是 P1。
- **Operations A199 audit-before-delete admission and finalizer release（2026-07-18）**：
  `operationqueue.Submit` 创建每个新 operation 时默认加入
  `dbaas.kubebrain.io/operation-audit` finalizer；提前 DELETE 只设置 deletionTimestamp，
  CR 保持 Terminating，终态 spec/status/UID 仍可被归档。新增 fail-closed
  `ValidatingAdmissionPolicy`/binding：直接 CREATE 缺 finalizer 被拒绝；UPDATE 移除
  finalizer 时必须是 Succeeded/Failed，并同时携带 64 位 archive receipt/artifact
  SHA-256 与非空 exact version ID。约束不能放 CRD CEL，因为 CRD validation 的 metadata
  类型不暴露 finalizers/annotations；真实 server-side 编译首先发现并拒绝该错误方案，
  随后迁移到 admission policy。

  `operation-audit --action release` 在移除 finalizer 前重新严格解析本地 artifact 和
  `kubebrain.object-operation-audit.receipt.v1`，要求 receipt 的 operation ID/UID/
  instance/type/phase/execution receipt、artifact SHA/bytes 全部匹配，再从 API Server
  读取当前终态 CR 并逐字段比对。通过后以单次 resourceVersion update 写入 receipt SHA、
  artifact SHA、version ID annotations 并只移除自身 finalizer，保留其他控制器
  finalizer；冲突、错误 UID、终态漂移、receipt 非 canonical 或缺证据都 fail closed。
  归档脚本在 exact-version 下载和 retention 复核完成后才调用 release，崩溃重试可复用
  同一 Object Lock receipt。

  独立 archiver ServiceAccount 的 namespaced Role 只有 operation get/update；实际
  subresource 检查 status update=`no`，create/delete/Lease update 也均为 `no`。CRD
  immutable-spec CEL 继续阻止该主资源 update 权限修改 operation 参数。结构与状态机测试
  覆盖默认 finalizer、其他 finalizer 保留、release 幂等、错误 receipt UID、当前终态漂移、
  admission 资源范围/Fail/Deny 和精确 RBAC；production test、race、vet、server-side
  dry-run 和 shell syntax 全部通过。

  真实 `kind-kubebrain-dbaas` 首次提前删除后 CR 保持
  deletionTimestamp+finalizer；这次还发现 Kubernetes 将删除中 CR generation 从 1
  推到 2，而终态 observedGeneration 保持 1，artifact gate 因而修正为
  `0 < observedGeneration <= generation`（spec 仍不可变）。归档 exact version
  `0fccf959-5029-435f-8753-8e5974bba086` 后 release 放行同一次删除。第二轮使用 10 分钟
  短期 archiver ServiceAccount token 独立完成 capture/archive/release，operation UID
  `01628f73-558b-4008-905f-0bf1901a6455`、artifact digest
  `357b410de53c31320c24d31224351377b9eaba77d6d7e76a76e9faf2bb89b79f`、exact version
  `14def952-ec44-44c4-bb61-3630e471ffd1`。直接无 finalizer CREATE 和无 archive annotations
  removal 均被 admission 拒绝；两份 COMPLIANCE object 到期后按 exact version 清理，
  A199 object/version、operation 和 Lease 残留均为 0。
- **Operations A200 exact-version bucket inventory reconciliation（2026-07-18）**：
  A188 objectstore module 新增 `manifest` 与 `inventory` 动作。
  `kubebrain.object-inventory-manifest.v1` 从已验证的 logical backup/audit archive
  receipts 生成，不接受手工不受约束的远端列表；绑定稳定 Object Store ID、bucket、
  严格 prefix，并为每个期望对象绑定 artifact format、key、exact version ID、
  SHA-256、bytes、retention mode/absolute retain-until。builder 自动按 key/version
  排序、拒绝重复/跨 prefix/不同 store 或 bucket，并以 canonical 单行 JSON、0600、
  file/directory fsync 和不可覆盖写发布。

  inventory 使用 `ListObjectVersions` 的 key+version 双 marker 分页扫描完整 prefix，
  要求页游标严格前进且 version identity 不重复；远端 version 集必须与 manifest
  精确相等，任何缺失或额外 version、任何 delete marker 都 fail closed。随后对每个
  exact version 执行 HeadObject，核对 size、format、Object Store ID、artifact digest、
  bytes 与 retain-until metadata，并用 GetObjectRetention 核对 mode/date。全部通过才签发
  canonical、不可覆盖的 `kubebrain.object-inventory.receipt.v1`，绑定 manifest SHA-256、
  expected/remote counts、delete marker=0 和检查时间；重试仍重做完整远端对账后复用原
  receipt。manifest 可为空，用于证明受管 prefix 无残留。

  fake S3 覆盖双 marker 分页、clean 幂等、缺失/额外 version、delete marker、
  metadata/retention 漂移、分页不前进、manifest 非 canonical/乱序/重复及错误 store；
  错误路径均不发布 receipt。独立 objectstore module test/race/vet、完整 production
  test、shell syntax 和 diff check 通过。真实 MinIO 在
  `operation-inventory-a200/` 先归档 2 个 COMPLIANCE operation versions，clean receipt
  返回 expected/remote=`2/2`、manifest digest
  `6a2c1c7635e57a0a4c2b50000e36c4e67fd4627d7db36b09e45b177d5588069e`。
  加入第三个未登记 version 后旧 manifest 非零退出且无 receipt；更新 3-version manifest
  后通过，再创建 delete marker 后同样拒绝且无 receipt。移除 marker 后最终 clean receipt
  expected/remote=`3/3`、manifest digest
  `c62034dd6c1c0d48f9c422a690d6b524af4c1d13c3656974f1d21672e5670211`、receipt digest
  `692d9258b5820160c6be3c1be3c1e3ef541cccd4e17caf3fa04fb108deb71708`。
  三个 exact versions 到期后按 manifest 清理，A200 prefix 与 operation 残留均为 0。
  lifecycle 删除必须先更新控制面期望清单；跨 bucket/account 汇总与定期调度仍属管理面。
- **Operations A201 dedicated namespace and credential boundary cleanup（2026-07-18）**：
  生产 KubeBrain 与 TiDB Cluster Namespace 清单新增
  `dbaas.kubebrain.io/instance` 和 `dbaas.kubebrain.io/dedicated=true` 边界标签；
  未同时匹配实例和 dedicated 标签的共享 Namespace 不可进入清理状态。扩展
  `uid-delete` 支持 cluster-scoped resource，Namespace 删除继续使用 API Server
  `DeleteOptions.preconditions.uid` 和 foreground propagation，不退化为仅按名称删除。

  新增 `hack/production/cleanup-instance-boundaries.sh` 的 prepare/delete/complete
  状态机。prepare 必须先验证 A187 `kubebrain.destroy.receipt.v1` 的实例、两个
  Namespace 和 `resources_absent=true`，冻结该回执 SHA-256、两个 Namespace UID，
  以及位于独立管理 Namespace、同时带 instance/credential 所有权标签的外部 Secret
  UID。delete 要求精确 `cleanup:<instance>:<cleanup-id>` 确认，重新验证回执摘要和
  全部 UID 后才删除 Secret 与 Namespace；complete 等待全部边界消失后签发不可覆盖、
  file/directory fsync 的 `kubebrain.boundary-cleanup.receipt.v1`。同名资源 UID
  变化、回执被替换、共享 Namespace、错误凭据标签和错误确认均 fail closed；NotFound
  可安全重试。

  mock 测试覆盖 prepare/delete/complete 与 receipt 幂等、集群级删除参数、错误确认、
  Namespace/Secret 所有权错误、两类 UID 漂移和销毁回执 digest 漂移。测试曾捕获
  `snapshot_secret` 位于 `printf` 命令替换时 Bash 忽略非零状态的 fail-open，现已改为
  显式赋值检查后才写入状态。完整 production test、目标 race、vet、shell syntax 和
  manifest test 通过；同时修正 worker RBAC 清单测试，使其精确验证既有 Lease
  create/get/update/delete 权限。

  真实 `kind-kubebrain-dbaas` 隔离演练创建 `a201-kb`、`a201-storage` 两个专属
  Namespace 和独立 `a201-control` 中两个凭据 Secret。错误确认非零退出且四个目标
  均保留；正确确认后四次 API 删除均接受准备阶段记录的真实 UID，completion receipt
  digest 绑定的 destroy receipt SHA-256 为
  `48eb3d658431b440bc807c386bc7cb9cfa1f9e9322593eb8520d7e095539af60`，
  `namespaces_absent=true`、`credentials_absent=true`。临时控制 Namespace 已清理；
  主 3 个 KubeBrain Pod 保持 Ready 且零重启。
- **Operations A202 exact-version backup lifecycle operation（2026-07-18）**：
  `KubeBrainOperation` 新增 `BackupDeletion` 类型和
  `hack/production/run-backup-deletion-operation.sh` executor。operation 参数文件除
  source upload receipt、删除前/后 inventory manifest 路径外，还必须固定三份输入的
  SHA-256；worker claim 后再次逐字节复核，避免仅冻结路径却允许控制面证据在执行期间
  被替换。删除前 manifest 必须精确包含 source receipt 的 key/version/artifact digest，
  删除后 manifest 必须不再包含该 exact version，且两份 manifest 必须属于相同稳定
  Object Store ID/bucket。

  executor 先运行 A200 exact-version inventory，证明控制面旧期望与远端完整一致；随后
  复用 A188 Object Lock delete，只有绝对 retain-until 到期并且远端 metadata/retention
  仍与 upload receipt 一致时才删除该 version；最后按已移除版本的新期望 manifest
  再次 inventory，拒绝额外 version、缺失 version 或 delete marker。成功 operation
  receipt `kubebrain.backup-deletion-operation.receipt.v1` 不可覆盖，并绑定 source
  receipt、pre/post manifest、pre/post inventory receipt、deletion receipt 的 SHA-256
  及 exact key/version。崩溃重试复用每阶段不可变 receipt，heartbeat 失败仍由 operation
  attempt fencing 中止。

  mock 测试覆盖三阶段顺序、完整重试、pre/delete/post 任一步失败 requeue、参数 digest
  漂移、manifest bytes 漂移、删除 receipt identity 伪造、post manifest 仍含目标版本、
  heartbeat fencing 和最终 receipt 幂等；CRD test 固定 `BackupDeletion` enum。真实
  归档演练还发现 operation audit artifact 类型白名单漏掉新类型，导致 finalizer 无法
  release；该路径保持 fail closed 且未上传错误审计，白名单和回归测试补齐后才继续。

  真实 `kind-kubebrain-dbaas` 向 MinIO `kubebrain-logical` 上传
  `backup-lifecycle-a202/a202-backup-1.jsonl` COMPLIANCE exact version
  `9de8224a-07a9-48cd-b7cb-7328393aae22`。保留期内 attempt 1 明确返回 retention 未到期并
  requeue；到期后 attempt 2 先取得 pre inventory expected/remote=`1/1`，删除 exact
  version，再取得 post inventory=`0/0`，operation Succeeded，execution receipt digest
  `02149fe13e88cd8b548a9dda8837819966326750200795b452488be5a8d167f7`。终态 operation
  随后完成 Object Lock 审计归档和 finalizer release；归档 version 到期后精确删除，
  空 inventory 证明 backup/audit prefix 均无 version/delete marker，operation/Lease
  残留为 0。主 3 KubeBrain、3 PD、3 TiKV 均 Ready 且零重启。
- **Operations A203 canonical object evidence parsing（2026-07-18）**：
  A202 lifecycle 审计发现 objectstore 模块的 backup upload、backup deletion 和
  operation audit receipt reader 仍使用宽松 `json.Unmarshal`；它虽然校验必要字段，
  但会接受未知字段、前导空白和其他非 canonical bytes，且不能给所有调用方提供一致的
  单一 JSON 值契约。inventory 和独立 operation-audit reader 已严格校验，因此同一
  inventory manifest builder 的不同 evidence 类型存在不一致。

  三类 reader 现统一使用 `json.Decoder.DisallowUnknownFields`，要求第二次 decode
  立即得到 EOF，并将结构重新 marshal 为 canonical 单行 JSON 与原始 bytes 逐字节比较；
  之后才执行 receipt 语义验证。这样参数文件固定的 SHA-256、不可覆盖本地 evidence 和
  manifest builder 读取的是同一唯一编码，未知控制面字段、尾随第二个 JSON 值、缩进或
  前导空白均 fail closed。atomic writer 的已有 receipt 幂等路径也经过同一严格 reader，
  不会把语义相同但字节不同的文件视为可复用证据。

  单元测试对 backup/deletion/audit 三类 receipt 分别覆盖 canonical 成功、前导空白、
  unknown field 和 trailing JSON；objectstore 全套 test/race/vet 与 A202 executor 回归
  通过。真实 A202 已删除 exact version 再次执行 delete，严格读取 canonical upload 和
  已有 deletion receipt 后幂等成功；canonical backup/audit receipts 均可生成 inventory
  manifest。对真实 upload receipt 注入 unknown field、对 audit receipt追加第二个 JSON
  值后均非零退出且不发布 manifest，远端状态未改变。
- **Operations A204 two-party high-risk operation approval（2026-07-18）**：
  现有 A196 submitter 虽已与 worker 分离，但仍能直接提交 Destroy、恢复切流、证书轮换和
  到期备份删除，worker 会立即 claim，缺少独立批准者。新增
  `kubebrain-operation-approver` ServiceAccount/Role/RoleBinding；该身份不能 create、
  delete、修改 status 或 Lease，只能 get/list/watch/update 已存在 operation。submitter
  继续只能 create/read，worker 继续只能读主资源、写 status 和持有 Lease，三个身份
  互不兼任且都没有扩大到数据面资源。

  `RestoreCutover`、`CertificateRotation`、`Destroy`、`BackupDeletion` 被列为高风险。
  新 operation 一律禁止携带预批准 annotation；未批准的高风险 operation 可以持久排队，
  但 queue 不 claim、不创建实例 Lease、也不消耗 attempt。新增
  `operationctl --action approve --approval-id <decision>`，只允许 Pending 高风险
  operation，写入 `approved-by` 和外部 decision ID；相同证据重试幂等，不同证据拒绝
  覆盖。admission 以 `request.userInfo.username` 要求首次批准者必须精确为独立 approver
  ServiceAccount，decision ID 必须是最长 128 字节 DNS-compatible ID，批准后两个字段
  不可撤销或修改。queue 还独立复核固定 approver username 和 ID，即使 admission
  被误删也不会执行任意伪造 annotation。终态 `kubebrain.operation-audit.v1` 把
  approved-by/approval-id 纳入 canonical artifact；四种高风险类型缺失或伪造审批证据
  均不能归档和 release finalizer，低风险类型也拒绝携带误导性的审批字段。

  单元测试覆盖四类高风险 operation 未批准不可 claim、批准后 attempt 1、伪造/半份
  approval 不生效、Pending-only、批准幂等/冲突、低风险拒绝多余批准和非法 decision ID；
  清单测试固定 approver 的精确 namespaced RBAC 及 admission 五条门禁。完整 production
  test、queue/operationaudit race、vet 和 API Server server-side CEL/RBAC dry-run 通过。

  真实 `kind-kubebrain-dbaas` 使用 submitter、approver、worker 三枚 10 分钟短期
  ServiceAccount token：submitter 创建未批准 Destroy 成功但无 update 权限；worker
  claim 返回无任务专用退出码 3 且 attempt 未增加；集群管理员伪造正确 approver 字符串
  仍被 `request.userInfo.username` 门禁拒绝；携带 approval 的 CREATE 也被拒绝。独立
  approver 写入 `change-a204` 后 worker 才取得 attempt 1。测试 operation 转 Failed 后
  approval 修改仍被拒绝，随后 Object Lock 审计归档、finalizer release 和 exact-version
  到期清理完成。第二个真实 Destroy 终态 artifact 明确保存固定 approver username 和
  `change-a204-audit`，归档 exact version
  `a80ded08-07e3-4edf-a4bf-180494c46f94` 到期后也已清理；operation/Lease/对象残留为 0。
  `auth can-i --subresource=status` 进一步确认 approver 的 status get/update/patch 均为
  `no`。该项完成 Kubernetes 原生两方审批门禁；
  外部管理 API 的 OIDC、租户到 instance 授权和审批系统 decision 签名/回查仍属 P1。
- **Compatibility A205 official STM and cross-key Txn fast paths（2026-07-18）**：
  对照 `/root/etcd/tests/integration/v3_stm_test.go` 增加官方
  `client/v3/concurrency.NewSTM` 黑盒差分，覆盖 RepeatableReads 新建、abort context
  不提交、并发删除触发 compare retry，以及 SerializableSnapshot 重复执行。修复前参考
  etcd 的删除冲突 callback 执行 2 次，真实 TiKV-backed KubeBrain 仅执行 1 次。
  根因是 `isCreate`、`isUpdate`、`isCompareDelete` 三个单键优化只检查 compare 与
  mutation 的形状，没有要求两者 key 相同；STM 读取 source key、写入 result key 的
  跨键 compare 因而绕过通用 Txn 路径，丢失 source conflict。

  三个 fast-path detector 现都要求 compare key 与 mutation key 逐字节相等；跨键请求
  统一进入通用原子 Txn。服务端回归覆盖不存在 guard 更新不同已有 key、mod guard 写入
  不同 key、mod guard 删除不同 key；官方 client 差分另固定上述四种跨键 create/update/
  delete/stale compare 结果。参考 etcd 与 3 KubeBrain/3 PD/3 TiKV 部署的 STM 和跨键
  差分连续 10 轮通过；10 worker、5 account 的并发 STM 转账守恒测试连续 5 轮通过，
  目标 server 与 client compatibility race 通过。共享服务上的差分不比较全局 revision
  增量，并在冲突场景前确认 serializable seed 可见，避免其他请求和 follower revision
  发布延迟污染场景，但仍明确断言首次 callback 读到 source 且提交必须重试。

  本轮镜像构建还暴露 `build-tikv.sh`/`build-badger.sh` 在严格 metadata 校验失败后仍会
  继续 `go build` 的 fail-open：无效 Git SHA 曾生成空 provenance 镜像。两个 wrapper
  现启用 `set -euo pipefail` 并安全引用路径；回归测试用 fake `go` 证明 metadata 失败
  后绝不调用 build。真实无效 SHA Docker build 非零失败；运行镜像
  `kubebrain:a205-stm-cross-key` 包含 Version 3.7.0、TiKV storage、完整 40 字节 Git SHA、
  UTC BuildTime 和 linux/amd64 metadata，三个 KubeBrain Pod Ready 且零重启。
- **Production A206 committed-source provenance and compat vet gate（2026-07-18）**：
  A205 后完整 compat module `go vet ./...` 暴露二进制 key disposable-endpoint probe
  仍用值复制 `RangeRequest`，会复制 protobuf `MessageState` 内部 mutex。probe 现改用
  `proto.Clone` 后再覆写 Limit/KeysOnly，不共享调用方 request 的 protobuf 状态；
  compat module 全量 vet 恢复通过。参考 etcd 与真实 TiKV-backed KubeBrain 的完整
  `hack/etcd-client-compat` 套件强制 uncached 运行 164.159 秒通过。

  同时纠正 A205 运行证据的 provenance 缺口：旧镜像包含未提交 A205 工作树代码，但 OCI
  revision 仍指向构建前提交，无法由 Git revision 重建同一源码。A206 先提交全部实现和
  vet 修复，再通过 `git archive HEAD` 创建不含用户未提交 `go.mod` 的干净构建上下文；
  镜像 `kubebrain:a206-provenance` 的 OCI revision 与二进制 `version` 均为
  `81af103e938e858fbaf08bcf6a4cf03b9136404a`，Version 3.7.0、Storage TiKV、
  Go 1.26.5、linux/amd64、UTC BuildTime 也逐项一致。镜像 ID 为
  `sha256:f0ca86de3cfd3263b061c7a2d2ccd961afe37a85cd33f51f8744cc70c9f97c0a`。

  kind 顺序滚动三个 KubeBrain Pod 后全部 Ready、零重启，3 PD/3 TiKV 同样 Ready、
  零重启；endpoint status 返回 3.7.0 且 proposal health 成功。二进制 key、STM、
  跨键 fast-shape Txn 差分在新镜像上连续 3 轮通过。后续生产发布必须从 committed
  source archive 或等价的 clean checkout 构建，不能仅给 dirty worktree 贴 HEAD
  revision 标签。
- **Compatibility A207 official client Watch fragmentation（2026-07-18）**：
  对照 `/root/etcd/tests/integration/clientv3/watch/watch_fragment_test.go` 补齐
  `clientv3.WithFragment` 的黑盒差分。既有 Watch control 差分只直接读取 raw gRPC
  response，证明多个大 `PrevKv` DELETE event 的 `Fragment=true/.../false` 标志与
  etcd 一致；它没有证明官方 client 能在单条聚合响应超过其 receive limit 时接收并
  重组 fragments。

  新场景在参考 etcd 与真实 TiKV-backed KubeBrain 分别写入 10 个 1 MiB value，从写入
  前 revision 建立 prefix history watch，并把独立 watcher client 的
  `MaxCallRecvMsgSize` 限为 1.5 MiB。单 event 可接收，但未 fragmentation 的约 10 MiB
  聚合响应必然得到 ResourceExhausted；两端启用 `WithFragment` 后均由官方 client
  重组为完整 10 events，连续 3 轮通过。最初用两个 800 KiB event 校准时参考 etcd
  也因 protobuf 编码后消息比 1.6 MiB limit 多 166 bytes 而拒绝，证明测试不能忽略
  wire encoding 开销；最终场景严格采用上游容量形状。

  带真实 reference/KubeBrain endpoint 的 Watch control + fragment compat race、
  compat module 全量 vet，以及服务端 fragment flag/limit 单测 10 轮通过。当前
  KubeBrain 已正确读取 `WatchCreateRequest.Fragment`、按配置阈值分片并仅在最后一片
  清除 Fragment；本轮未发现实现差异，只增加缺失的官方 client 接收上限证据。三个
  KubeBrain Pod 和 3 PD/3 TiKV 在测试后保持 Ready、零重启。
- **Compatibility A208 etcd 3.7 leasing client lock order（2026-07-18）**：
  对照 `/root/etcd/tests/integration/clientv3/lease/leasing_test.go` 首次引入
  `go.etcd.io/etcd/client/v3/leasing` 黑盒差分。场景组合两个 leasing KV client 的
  missing-key acquire、跨 client owner/cache invalidation、`WithPrevKV`、历史 revision、
  non-owner delete，以及同一 owner 对同一 data key 的 8 个并发 Put；最终缓存必须返回
  最高 ModRevision、Version=11 且删除后不可继续读到旧值。

  修复前参考 etcd 快速完成，但真实 TiKV-backed KubeBrain 的并发阶段多笔 Txn 每 10 秒
  才返回 DeadlineExceeded；即使 client 重试后约 32.65 秒偶尔收敛，也违反上游
  `TestLeasingConcurrentPut` 的进度契约。直接连接 leader 仍复现，排除了 follower proxy。
  根因是 atomic Txn 采用 `mutation stripe -> backend logical RLock`，而 leasing owner
  竞争会混入需要 range/read staged execution 的 Txn，后者采用
  `backend logical exclusive -> mutation stripe`；两类请求命中相同 data key 时形成锁序
  反转，直到 unary deadline 打破等待。

  `backendShim.BeginRangeTxn` 在取得 backend logical exclusive 后，现把返回 context 标记
  为已覆盖全部 mutation stripes。staged transaction 内的 point Put/Delete/TxnApply
  因此不再二次取得 stripe；全局 exclusive 已保证其 read/guard/write 窗口内没有其他
  backend writer，atomic writer 即使先取得 stripe也必须先完成 logical RLock 临界区，
  不再形成等待环。确定性单测固定 range transaction 内 TxnApply 不得重取 stripe，原有
  same-key serialization/cancel/BeginMutation ownership 回归连续 20 轮及目标 race
  10 轮通过，完整 server test 通过。

  leasing 并发阶段现使用独立 5 秒 budget；参考 etcd 与修复后的 KubeBrain 差分连续
  10 轮通过，每轮总场景从旧版约 32 秒降到 0.8–1.7 秒。真实 endpoint 的差分 race
  3 轮通过；STM、无条件 Txn、Put/Delete 三组并发回归各 5 轮合计 105.009 秒通过。
  committed-source 镜像 `kubebrain:a208-leasing-lock-order` revision
  `3d72aca1284f0dc259264c31e898763fce81ee96`、image ID
  `sha256:fbc158669052addea02d2567c7ed8402920dad321021036914b4f066dbbe1bc2`；
  顺序滚动后 3 KubeBrain、3 PD、3 TiKV 均 Ready 且零重启，endpoint proposal health
  成功。
- **Compatibility A209 leasing range/Txn ownership（2026-07-18）**：
  继续对照 `/root/etcd/tests/integration/clientv3/lease/leasing_test.go` 的
  `TestLeasingTxnOwnerGetRange`、`TestLeasingTxnOwnerDeleteRange`、
  `TestLeasingTxnRangeCmp` 和 nested non-owner put 契约，补齐 point-key A208 未覆盖的
  prefix/range 路径。差分使用两个独立 leasing client：先缓存 4-key prefix（其中一个
  key Version=2），验证 `Version(prefix)==1` range compare 必须失败；另一 client 再用
  nested Txn 同 revision 更新已有 key并创建新 key，原 owner 的 prefix cache 必须看到
  5 个最新 value。

  随后 non-owner leasing client 对该 prefix 执行 DeleteRange。返回 Deleted=5，历史
  watch 收到 5 个 DELETE event 且所有 ModRevision 等于 delete header revision；原 owner
  再读 prefix 为空，范围外 sentinel 保持不变。该场景同时穿过 leasing 的 range ownership
  revoke、guard range compare、nested Txn response rebuild、staged range execution、
  atomic range delete、watch history 和跨 client cache invalidation。

  参考 etcd 与真实 TiKV-backed KubeBrain 连续 5 轮通过；point/range 两组 leasing
  差分在真实 endpoint 下 race 3 轮通过，compat module 全量 vet 通过，服务端
  BeginRangeTxn ownership、nested/range Txn 回归连续 20 轮通过。A208 的 logical
  exclusive context 覆盖全部 mutation stripes 已足以处理这些范围路径，本轮未发现新的
  服务端差异，只补齐上游 leasing range 客户端证据。运行镜像继续为 committed-source
  `kubebrain:a208-leasing-lock-order`；3 KubeBrain、3 PD、3 TiKV 保持 Ready、零重启。
- **Compatibility A210 leasing session expiry/recovery（2026-07-18）**：
  对照 `/root/etcd/tests/integration/clientv3/lease/leasing_test.go` 的
  `TestLeasingSessionExpire` 和 session cancel 路径，新增短 TTL leasing client
  黑盒差分。场景先由第一个 client 缓存旧值并取得 owner lease，再由独立官方 client
  显式 Revoke 该 lease，以确定性方式模拟 session 失效；确认 owner key 删除后，第二个
  leasing client 写入新值，第一个 client 必须丢弃旧缓存、返回新值并建立不同于旧 lease
  的新 owner。

  新 owner 的创建由 leasing client 异步完成，因此不能在首次新值读取后立即断言 owner
  已出现；初版即时检查在参考 etcd 与 KubeBrain 间呈现时序差异，但在有界轮询中两者均
  完成重建。这是观测时机差异而非持久语义缺口。最终差分连续 5 轮通过，point/range/
  session-expiry 三组 leasing 差分在真实 endpoint 下 race 3 轮通过；keepalive 零 TTL、
  过期响应顺序和 orphaned concurrency session handoff 回归各 5 轮通过，compat module
  全量 vet 通过。本轮无需服务端修改，运行镜像继续为
  `kubebrain:a208-leasing-lock-order`。
- **Compatibility A211 leasing atomic multi-key cache（2026-07-18）**：
  对照 `/root/etcd/tests/integration/clientv3/lease/leasing_test.go` 的
  `TestLeasingTxnAtomicCache`，新增 8-key 并发 leasing Txn 黑盒差分。4 个 writer
  各执行 8 次事务，每次在同一 revision 将全部 key 更新为同一 generation；4 个 reader
  持续通过 leasing Txn 读取全部 key。每笔读事务的 8 个 KV 必须具有相同 ModRevision，
  最终全部 value 也必须属于同一 generation，且所有 32 笔写事务完成并保持 reader
  进展。

  该场景同时覆盖多 key owner/cache 建立、并发 staged Txn、响应重建、watch 驱动缓存
  更新，以及 A208 修复后的 logical exclusive/mutation stripe 锁序。参考 etcd 与真实
  TiKV-backed KubeBrain 连续 5 轮通过，全程未出现 mixed-revision read；连同 A208-A210
  四组 leasing 差分在真实 endpoint 下 race 3 轮通过，compat module 全量 vet 通过，
  mutation/range transaction 服务端回归连续 10 轮通过。本轮未发现新的服务端语义差异，
  运行镜像继续为 `kubebrain:a208-leasing-lock-order`。
- **Compatibility A212 leasing reconnect across compaction（2026-07-18）**：
  对照 `/root/etcd/tests/integration/clientv3/lease/leasing_test.go` 的
  `TestLeasingReconnectOwnerRevokeCompact`，为外部真实 endpoint 新增测试专用 TCP
  bridge。bridge 可只 blackhole 第一个 leasing client 的双向连接，在 socket read 后
  丢弃流量并累计字节数；恢复时关闭全部旧连接，迫使 gRPC/watch 从断连前 revision
  重建，第二个 client 始终直连。

  差分先由第一个 client 读取 missing key 并建立 owner/watch，再进入 blackhole；
  直连 client 推进两个 revision 并 compact 到最新 revision。测试在恢复前硬断言确有
  watch/连接流量被丢弃，排除无故障假阳性；恢复连接后，第二个 leasing client 写入新值，
  第一个 client 必须跨过 compacted watch revision 完成 owner/cache reconciliation，
  并返回与直读一致的新值。

  带实际 dropped-byte 证据的 reference etcd 与真实 TiKV-backed KubeBrain 差分连续
  5 轮通过；A208-A212 五组 leasing 差分在真实 endpoint 下 race 3 轮通过，compat
  module 全量 vet 通过。backend compaction/watch/lease/range transaction 回归连续
  10 轮约 98.4 秒、server/etcd 对应回归连续 10 轮约 110.5 秒通过。本轮未发现新的服务端
  语义差异，运行镜像继续为 `kubebrain:a208-leasing-lock-order`。
- **Compatibility A213 leasing ambiguous owner write（2026-07-18）**：
  对照 `/root/etcd/tests/integration/clientv3/lease/leasing_test.go` 的
  `TestLeasingReconnectOwnerConsistency`，把 A212 TCP bridge 扩展为定向
  server-to-client response blackhole。与上游随机 DropConnections 相比，本轮固定让
  owner 请求完整到达服务端、由直连 client 观察到新值已提交，同时丢弃 Txn response，
  直到 owner RPC 返回 DeadlineExceeded，从而确定性覆盖 committed-but-unacknowledged
  的 ambiguous write。

  恢复时 bridge 关闭旧连接触发 gRPC/watch 重建；owner leasing Get 必须丢弃可能残留的
  旧缓存，并最终与直读一致地返回已提交值。测试分别硬断言服务端已应用写入、bridge
  实际丢弃响应字节、owner 调用确实超时和恢复后 cache/server 一致，避免把未发送请求、
  正常响应或最终旧值误记为通过。

  reference etcd 与真实 TiKV-backed KubeBrain 连续 5 轮通过；A208-A213 六组 leasing
  差分在真实 endpoint 下 race 3 轮约 43.9 秒通过，compat module 全量 vet 通过。
  backend Txn/mutation/range/watch 回归连续 10 轮约 48.9 秒、server/etcd 对应回归连续
  10 轮约 51.3 秒通过。本轮未发现新的服务端语义差异，运行镜像继续为
  `kubebrain:a208-leasing-lock-order`。
- **Compatibility A214 leasing ambiguous mutation matrix（2026-07-18）**：
  继续展开上游 `TestLeasingReconnectOwnerConsistency` 的随机操作矩阵，在 A213
  owner Put 之外，确定性覆盖 owner Delete、Txn(Get+Put)、Txn(Get+Delete)、
  `Do(Put)` 和 `Do(Delete)`。每项均先建立 owner/cache，再仅 blackhole
  server-to-client response；直连 client 必须先证明目标 mutation 已提交，bridge
  必须记录新增丢弃字节，owner 调用必须 DeadlineExceeded，恢复后 leasing Get 的完整
  key/value/revision/version/lease 元数据必须与直读一致。

  leasing prefix delete 在最终 Txn 前包含 Range RPC；纯 TCP response blackhole 会先
  截断该前置响应，因此不能确定性制造“range delete 已提交但最终 response 丢失”。
  本轮没有用延时切换制造时序型假证据：prefix/range delete 的原子语义由 A209 覆盖，
  compacted watch 断连恢复由 A212 覆盖，而 committed-but-unacknowledged 结论严格限于
  本轮五项和 A213 Put。

  reference etcd 与真实 TiKV-backed KubeBrain 五操作矩阵连续 3 轮约 27.3 秒通过；
  A213/A214 两组 ambiguous mutation 差分在真实 endpoint 下 race 3 轮约 34.5 秒通过，
  compat module 全量 vet 通过。backend Txn/Delete/mutation/range transaction 回归
  连续 10 轮约 38.3 秒、server/etcd 对应回归连续 10 轮约 58.4 秒通过。本轮未发现新的
  服务端语义差异，运行镜像继续为 `kubebrain:a208-leasing-lock-order`。
- **Compatibility A215 leasing operations under connection churn（2026-07-18）**：
  对照上游 `TestLeasingReconnectTxn` 和 `TestLeasingReconnectNonOwnerGet`，扩展 A212
  TCP bridge 以统计并主动关闭活动连接。missing-key 条件 Txn 在 5 次连续 socket drop
  窗口内必须自行重连并成功返回空 Get 分支；随后预置 5 个偶数 key，对 10 个 existing/
  missing key 分别在 3 次 socket drop 窗口内执行 leasing Get，每次均必须在 5 秒 budget
  内完成。

  每个 Txn/Get 都先通过 started barrier 确认首次 socket drop 已完成，其余 drop 再与
  调用并发；窗口结束后还比较 drop 前后计数。每个 Get 的完整 key/value/create
  revision/mod revision/version/lease 元数据必须与独立直连 client 一致。初版 20/10
  次断连虽连续 5 轮通过，但产生无必要的连接风暴和大量 retry 日志；最终 5/3 次配置
  保留实际故障重叠证据并把 5 轮耗时收敛到约 14.9 秒。

  reference etcd 与真实 TiKV-backed KubeBrain 最终场景连续 5 轮通过；加入首次断连
  barrier 后 A215 差分在真实 endpoint 下 race 3 轮约 12.8 秒通过，compat module 全量
  vet 通过。backend Txn/watch/range transaction/mutation 回归连续 10 轮约 49.0 秒、
  server/etcd 对应回归连续 10 轮约 51.5 秒通过。本轮未发现新的服务端语义差异，运行
  镜像继续为 `kubebrain:a208-leasing-lock-order`。
- **Compatibility A216 leasing offline compare/deep branches（2026-07-18）**：
  对照上游 `TestLeasingTxnOwnerIf`、`TestLeasingDo` 和
  `TestLeasingTxnOwnerPutBranch`，新增三段确定性差分。第一段先缓存 owner key，再将
  client 双向 blackhole；Value/CreateRevision/ModRevision/Version 各一组 true/false
  compare 共 8 项必须在 1 秒 budget 内完全从缓存判定，true 分支返回一个 Get response，
  false 分支不返回 Then response。

  第二段依次通过 leasing `Do` 执行空 Txn、Get、Put、prefix Delete 和空 Txn，返回值
  必须暴露与输入 operation 匹配的 typed response。第三段构造深度 3、15 key 的固定
  nested Txn tree，交替选择 Then/Else；仅选中路径 4 个 key 可从 initial 更新为
  then/else/leaf，全部选中 key ModRevision 必须等于顶层 Txn header revision，未选中
  key 保持 initial，且每个 leasing cache KV 的完整元数据与直读一致。

  reference etcd 与真实 TiKV-backed KubeBrain 连续 5 轮约 15.7 秒通过；A209
  leasing-range 与 A216 branching 差分在真实 endpoint 下 race 3 轮约 11.5 秒通过，
  compat module 全量 vet 通过。backend nested/Txn/branch/range transaction 回归连续
  10 轮约 35.2 秒、server/etcd 对应回归连续 10 轮约 18.0 秒通过。本轮未发现新的服务端
  语义差异，运行镜像继续为 `kubebrain:a208-leasing-lock-order`。
- **Compatibility A217 leasing range-delete bounds/contention（2026-07-18）**：
  对照上游 `TestLeasingDeleteRangeBounds`、
  `TestLeasingDeleteRangeContendTxn` 和 `TestLeaseDeleteRangeContendDel`。边界场景由
  reader leasing client 缓存 `j/m` 并建立 owner，另一 client 删除 `k*`；`j/m` value
  必须保留，owner namespace 中两个 lease key 也必须仍存在，证明 range ownership
  计算没有越过请求边界。

  争用场景分别执行直接 prefix Delete 和 nested Txn 内 prefix Delete。独立 writer
  在删除前已完成至少一次 Put/Get，并持续轮转更新 8 个 key；删除返回后通过非取消
  stop channel 让当前操作自然完成，避免把预期 Canceled 重试混入证据。无论某个 key
  最终被删除还是被并发 Put 重建，writer leasing cache 的完整 key/value/revision/
  version/lease 元数据都必须逐 key 与服务端直读一致，两类 `Do` response 也必须分别
  保留 Delete/Txn 类型。

  初版带 context cancel 的 reference/KubeBrain 差分连续 5 轮约 33.4 秒通过；最终无
  cancel 噪声版本连续 3 轮约 20.9 秒通过。A209 range 与 A217 range-contention 差分
  在真实 endpoint 下 race 3 轮约 24.2 秒通过，compat module 全量 vet 通过。backend
  DeleteRange/range transaction/mutation/TxnApply 回归连续 10 轮约 38.0 秒、
  server/etcd 对应回归连续 10 轮约 16.2 秒通过。本轮未发现新的服务端语义差异，运行
  镜像继续为 `kubebrain:a208-leasing-lock-order`。
- **Compatibility A218 leasing Put/Get/Delete concurrent progress（2026-07-18）**：
  对照上游 `TestLeasingPutGetDeleteConcurrent`，新增原尺度差分：创建 16 个 leasing
  client，由 16 个 worker 并发遍历全部 client；每个序列依次 Put、等待 1ms、Get、
  Delete、等待 2ms。reference etcd 与真实 TiKV-backed KubeBrain 都必须完成全部 256
  个序列，且最终 leasing Get 与直连 Get 均为空。

  旧 `kubebrain:a208-leasing-lock-order` 单轮约 58.2 秒、隔离 race 单轮约 59.2 秒
  通过，但隔离连续 2 轮时第二轮约 101 秒返回
  `Unknown: uncertain error: execution result undetermined`。根因是服务端无条件用
  10 秒 `unaryRpcTimeout` 包裹请求，即使客户端已显式提供 120 秒 deadline，也会在
  高争用 TiKV 2PC 尚未完成时提前取消 commit，并把 TiKV result-undetermined 暴露给
  etcd client。修复后，服务端保留任意客户端 deadline，仅对没有 deadline 的请求
  应用 10 秒兜底；单元测试同时覆盖无 deadline、500ms 短 deadline 和 30 秒长
  deadline，并以普通模式 20 轮、race 模式 5 轮通过。

  候选镜像 `kubebrain:a218-client-deadline-candidate` 在原 120 秒测试保护预算下首轮
  119.8 秒通过，后两轮正确收敛为 `DeadlineExceeded`，没有再出现 uncertain result；
  这仍暴露单节点 kind 环境连续满争用下的吞吐边界。由于上游使用 `t.Context()` 而非
  120 秒短 deadline，最终差分改用 5 分钟防挂死预算；完整差分约 90.4 秒通过，隔离
  race 约 26.9 秒通过，256 个序列及最终状态均与 reference etcd 一致。compat module
  全量 vet、`pkg/storage/...` 与 `pkg/server/etcd/...` 全量测试通过；三个运行副本
  全程 Ready、零重启，压测后 endpoint proposal 健康。提交 `e2cbc55` 的最终镜像
  `kubebrain:a218-client-deadline`（image ID `sha256:ef0d22946a5c...`、OCI revision
  `e2cbc554bfa77c59dbae6f7a885df56d83088781`）完成三副本滚动更新后，制品级完整差分
  再次约 35.6 秒通过。
- **Compatibility A219 leasing cache isolation/options/TTL bypass（2026-07-18）**：
  对照上游 `TestLeasingOverwriteResponse`、`TestLeasingOwnerPutResponse`、
  `TestLeasingGetWithOpts` 和 `TestLeasingGetNoLeaseTTL`，新增确定性组合差分。先让
  leasing client 取得普通 key 所有权，篡改第一次 Get 返回对象中的 key/value byte
  slice，再次 Get 必须仍返回原值，证明调用方无法通过 response alias 污染内部缓存。
  owner Put 后进入 TCP 双向 blackhole；离线 Get 的 value/version/mod revision 必须与
  Put response 一致，keys-only、count-only、limit、key sort、min/max create revision、
  min/max mod revision 和 serializable 选项都必须由本地 owner cache 正确处理。

  同一场景还通过直连 client 写入附带 60 秒 lease 的业务 key。在线首次读取成功后，
  blackhole 窗口内再次 leasing Get 必须耗尽 500ms deadline，且 bridge 必须记录实际
  丢弃流量，证明带 TTL 的业务 key 没有被错误视为可离线服务的 owner cache 数据。
  reference etcd 与真实 TiKV-backed KubeBrain 完整差分连续 5 轮约 6.1 秒通过，race
  3 轮约 5.1 秒通过；compat module 全量 vet、server/etcd 相关 KV/Txn/Range/Lease
  回归 3 轮通过。本轮未发现新的服务端语义差异，运行镜像继续为
  `kubebrain:a218-client-deadline`。
- **Compatibility A220 leasing canceled/non-owner nested Txn（2026-07-18）**：
  对照上游 `TestLeasingTxnCancel` 和 `TestLeasingTxnNonOwnerPut`，新增取消与跨 owner
  原子失效组合差分。owner client 先缓存 3 个 key 并建立 ownership，再通过专属 TCP
  bridge 双向 blackhole；non-owner leasing client 对其中一个 key 发起 Txn Put，并在
  250ms 后取消。调用必须返回 `context.Canceled`，bridge 必须记录实际丢弃流量，直读
  value 必须仍为 initial，证明请求没有在返回 canceled 后暗中提交。

  恢复 owner 连接并确认旧值后，non-owner client 在单笔 Txn 中执行一层 nested Txn
  Put 和两个顶层 Put。响应必须成功且保留 3 个 response；prefix watch 必须观察到 3
  个 Put 全部使用顶层 Txn header revision。原 owner leasing cache 随后逐 key 读取的
  updated value/mod revision 必须与直连读取一致，证明 nested write 能原子撤销其他
  client 的 owner cache，而非只处理顶层 operation。

  reference etcd 与真实 TiKV-backed KubeBrain 完整差分连续 5 轮约 7.3 秒通过，race
  3 轮约 5.8 秒通过；compat module 全量 vet、server/etcd 相关 KV/Txn/Range/Lease/
  Watch 回归 3 轮通过。本轮未发现新的服务端语义差异，运行镜像继续为
  `kubebrain:a218-client-deadline`。
- **Compatibility A221 leasing owner delete with open upper bound（2026-07-18）**：
  对照上游 `TestLeasingOwnerDeleteFrom`，新增 `WithFromKey` owner-delete 差分。由于
  from-key 是全局开放范围，共享 DBaaS endpoint 上不能使用普通 ASCII 测试前缀；用例
  把数据放入双 `0xff` 高位二进制命名空间，并让 owner metadata 排在业务数据之前，
  从而只删除本用例创建的尾部 key。删除必须返回 typed Delete response 和 Deleted=3，
  三条 watch delete event 必须共享 response header revision；范围前的 key/owner 必须
  保留，范围内 leasing cache 与直读都必须为空，owner metadata 数量也必须与 reference
  etcd 一致。

  旧 `kubebrain:a218-client-deadline` 确定性返回
  `Unknown: invalid range end`。etcd leasing client 会把业务 `{0}` 开放上界拼接到
  owner prefix，形成 `ownerPrefix + "\x00"`；该值不再是开放上界，且字典序低于
  `ownerPrefix + dataKey`，所以 owner guard 是反向空 range。普通 Range、DeleteRange
  和 staged Txn 已短路这种空区间，但 `evalRangeCompareAtRevision` 仍直接调用
  backend.List，导致 TiKV backend 的 `invalid range end` 泄漏。修复后反向 range
  compare 使用 etcd 的空集合规则：VALUE compare 为 false，其他 target 按不存在 key
  的零值比较；回归同时验证 leasing 使用的 `CreateRevision < 1` 为 true，避免把所有
  空 compare 粗略处理成同一结果。

  候选镜像 `kubebrain:a221-empty-range-compare-candidate` 完成三副本滚动更新后，真实
  TiKV-backed 差分首轮约 1.8 秒、连续 5 轮约 8.7 秒、race 3 轮约 4.5 秒通过。聚焦
  server race 10 轮、`pkg/storage/...` 与 `pkg/server/etcd/...` 全量测试、compat
  module 全量 vet 通过；reference/KubeBrain 双 `0xff` 测试命名空间均确认零残留，
  三个运行副本 Ready、零重启。提交 `9c70005` 的最终镜像
  `kubebrain:a221-empty-range-compare`（image ID `sha256:4f3acf823ffa...`、OCI
  revision `9c700059633f5cb1eb68d77dbc4dfabfa90e3962`）完成滚动更新后，制品级差分
  再次约 0.72 秒通过。
- **Compatibility A222 plain Put at-most-once under ambiguous response（2026-07-18）**：
  对照上游 `TestKVPutAtMostOnce`，为普通 KV（非 leasing wrapper）新增确定性
  at-most-once 差分。每轮先通过 TCP bridge Get 预热并确认连接 ready，再仅 blackhole
  server-to-client 响应；Put request 可到达 reference/KubeBrain 并提交，但客户端在
  750ms deadline 后只能得到 `DeadlineExceeded`。bridge 必须记录新增丢弃字节，解除
  黑洞后 direct client 必须观察到该轮唯一 value，且 KV version 相对上一轮必须精确
  `+1`。这同时排除“请求未到服务端”的假阳性和 client/proxy/backend uncertain retry
  导致同一逻辑 Put 重复提交。

  每个双端场景连续制造 6 次已提交但响应丢失的 ambiguous Put；reference etcd 与真实
  TiKV-backed KubeBrain 首轮约 9.3 秒、连续 3 轮约 28.0 秒、race 2 轮约 19.9 秒
  通过，全部逻辑操作均只推进一次 version。compat module 全量 vet、server/etcd
  Put/Txn/Mutation/Leadership 相关回归 3 轮通过；三个运行副本 Ready、零重启，endpoint
  proposal 健康。本轮未发现新的服务端语义差异，运行镜像继续为
  `kubebrain:a221-empty-range-compare`。
- **Compatibility A223 nested multi-key Txn at-most-once under ambiguous response（2026-07-18）**：
  对照上游 `TestTxnWriteFail` 的事务写失败契约，将 A222 的确定性响应黑洞模型扩展到
  嵌套、多 key Txn。每端先在同一事务中创建三个 key；每轮通过 bridge 预热连接后，
  一个嵌套 Txn 写前两个 key，外层 Txn 再写第三个 key。bridge 仅丢弃 server-to-client
  响应，客户端必须在 750ms 后得到 `DeadlineExceeded` 且 bridge 必须观测到丢弃字节；
  direct client 随后必须看到三个新 value，每个 key version 相对上一轮精确 `+1`，
  且三个 key 的 ModRevision 完全相同。该组合同时验证 ambiguous response 不会触发
  重复提交，并验证嵌套操作仍保持单事务原子 revision。

  reference etcd 与真实 TiKV-backed KubeBrain 每端连续制造 4 次已提交但响应丢失的
  ambiguous Txn；首轮约 6.6 秒、连续 3 轮约 19.6 秒、race 2 轮约 14.3 秒通过。
  compat module 全量 vet、server/etcd Txn/Mutation/Leadership 相关回归 3 轮通过；
  三个运行副本 Ready、零重启，endpoint proposal 健康。本轮未发现新的服务端语义
  差异，无需重建服务端制品，运行镜像继续为 `kubebrain:a221-empty-range-compare`。
- **Compatibility A224 failed Put followed by same-client Get retry（2026-07-18）**：
  对照上游 `TestKVPutFailGetRetry`，补齐与 A222/A223 响应丢失场景互补的 request 丢失
  差分。每轮先用 bridged client 完成 Get 预热，再启用双向 blackhole，使 Put request
  字节确定被 bridge 丢弃并在 750ms 后返回 `DeadlineExceeded`。仍处于黑洞期间，
  direct client 必须确认目标 key 不存在；解除黑洞会主动断开旧连接，原 bridged client
  随后的 Get 必须在 5 秒内重连成功并同样返回空结果。每轮使用独立 key，避免前一轮
  连接状态或数据结果掩盖幽灵写入。

  reference etcd 与真实 TiKV-backed KubeBrain 每端连续执行 4 轮确定失败 Put；首轮约
  6.2 秒、连续 3 轮约 18.8 秒、race 2 轮约 13.7 秒通过。A222-A224 响应丢失/请求丢失
  组合连续 2 轮约 44.1 秒通过，compat module 全量 vet、server/etcd Put/Txn/Mutation/
  Leadership/Unary/Deadline 相关回归 3 轮通过；双端测试 prefix 均为零残留，三个运行
  副本 Ready、零重启，endpoint proposal 健康。本轮未发现新的服务端语义差异，无需
  重建服务端制品，运行镜像继续为 `kubebrain:a221-empty-range-compare`。
- **Compatibility A225 in-flight Get cancellation preserves transport（2026-07-18）**：
  对照上游 `TestKVGetCancel`，把仅使用预取消 context 的本地 client 检查强化为真实
  in-flight RPC 差分。每端先通过专属 TCP bridge 建立且确认恰好一条 transport；每轮
  只 blackhole server-to-client 流量，等待 bridge 确认 Range response 字节已被丢弃后
  再取消 context。Get 必须返回 `context.Canceled`；bridge 随后只恢复转发、不主动断开
  连接，client 的 active gRPC connection 对象、bridge accepted connection 总数和主动
  drop 总数必须全部不变。紧接着同一 client/transport 执行 Put 与 Get，必须读到该轮
  新值，证明单个 canceled HTTP/2 stream 不会污染共享连接上的后续 RPC。

  reference etcd 与真实 TiKV-backed KubeBrain 每端连续执行 4 轮 in-flight cancel；
  首轮约 0.45 秒、连续 5 轮约 2.2 秒、race 3 轮约 2.5 秒通过。修改后的 bridge 对
  A222-A224 request/response blackhole 场景回归约 22.0 秒通过，compat module 全量
  vet、server/etcd Range/Unary/Deadline/Cancel/ReadBarrier/Leadership 相关回归 3 轮
  通过。本轮未发现新的服务端语义差异，无需重建服务端制品，运行镜像继续为
  `kubebrain:a221-empty-range-compare`。
- **Compatibility A226 clientv3 namespace range/Txn/Watch isolation（2026-07-18）**：
  对照上游 `tests/integration/clientv3/namespace_test.go` 与
  `tests/integration/v3_kv_test.go::TestKVWithEmptyValue`，新增 namespace wrapper
  端到端差分。每端在唯一物理 tenant prefix 下写入 `a/b/c`，并在 prefix 字典序后继
  `tenant0/` 放置相邻 key；namespaced `Get("", WithFromKey)` 必须只返回去前缀后的
  `a/b/c`。随后 namespaced Txn 通过 value compare，在 nested Txn 中更新 `a`、创建
  `d`，并由外层删除 `b`；nested Put 与 Delete 的 PrevKv key 必须递归去前缀，三条
  namespaced Watch event 必须暴露逻辑 key 且共享顶层 Txn revision。

  事务后 namespace 必须精确包含 `a/c/d`；`Delete("", WithFromKey, WithPrevKV)` 必须
  返回 Deleted=3 和去前缀后的三个 PrevKv，namespace 随后为空，而物理相邻 key 仍保留。
  reference etcd 与真实 TiKV-backed KubeBrain 首轮约 2.7 秒、连续 5 轮约 13.3 秒、
  race 3 轮约 8.7 秒通过；Namespace/TxnFromKey/LeasingFromKey 组合连续 3 轮约 13.3
  秒通过。upstream namespace package 连续 3 轮、compat module 全量 vet、server/etcd
  Range/Txn/Delete/Watch/Mutation/Leadership 相关回归 3 轮通过，双端测试 prefix 零
  残留。本轮未发现新的服务端语义差异，无需重建服务端制品，运行镜像继续为
  `kubebrain:a221-empty-range-compare`。
- **Compatibility A227 etcdctl make-mirror bidirectional release gate（2026-07-18）**：
  对照上游 `client/v3/mirror/syncer.go`、`tests/integration/clientv3/mirror_test.go`
  与 `etcdctl/ctlv3/command/make_mirror_command.go`，新增真实
  `/root/etcd/bin/etcdctl make-mirror` 发布门禁。小型场景分别执行 reference→KubeBrain
  和 KubeBrain→reference：两个 seed key 必须完成 SyncBase，随后源端同一 Txn 内更新
  `a`、删除 `b`、创建 `c`，目标端必须最终精确为 `a/c`，且两个存活 key 的 ModRevision
  相同，证明 make-mirror 按源 revision 将更新批次原子提交到目标。

  独立分页场景在 KubeBrain source 以受 `--max-txn-ops` 约束的批次创建 1001 个 key，
  强制跨越 mirror SyncBase 的固定 1000-key page；reference destination 必须完整收到
  `key-0000` 至 `key-1000`。双向小型场景首轮约 0.53 秒、连续 10 轮约 4.7 秒、race
  5 轮约 3.6 秒通过；分页场景包含强制成功的双端清理约 30.1 秒，race 约 38.1 秒通过。
  首次分页验证虽复制成功，但 10 秒 cleanup deadline 对真实 TiKV 大范围删除不足并留下
  双端各 1001 key；门禁现使用 60 秒清理预算且强制检查错误，旧残留已清除，最终双端
  prefix 均为零。

  `run-differential.sh` 现显式校验并传入 `ETCDCTL_BIN`，两个测试名纳入 `Differential`
  选择器，确保不是手工旁路测试。compat module 全量 vet、server/etcd Range/Watch/Txn/
  Delete/Mutation/Leadership 相关回归 3 轮通过。`docs/etcdctl_compatibility_cn.md`
  中 `make-mirror` 已从“非生产保证”提升为“支持”；跨区域长期复制仍需独立 soak，不能
  由该确定性发布门禁替代。本轮未发现服务端语义差异，运行镜像继续为
  `kubebrain:a221-empty-range-compare`。
- **Compatibility A228 authenticated make-mirror on isolated keyspace（2026-07-18）**：
  对照上游 `tests/integration/clientv3/mirror_auth_test.go`，扩展 A227 的真实 CLI 门禁
  到 source/destination 双端 RBAC。测试只接受显式
  `KUBEBRAIN_AUTH_MIRROR_ENDPOINT`，并拒绝其等于共享主 endpoint；两端在任何 mutation
  前必须 auth-disabled 且 user list 为空。每端创建 root 与仅对
  `/dbaas-auth-mirror/` 有 ReadWrite 权限的 `mirror-syncer`，启用 auth 后先证明匿名
  Range 返回标准 `ErrUserEmpty`，再以 etcdctl `--user` 与 `--dest-user` 分别执行
  reference→KubeBrain、KubeBrain→reference。

  两个方向均验证两个 seed key 的基线同步，以及源端同 revision 更新 `a`、删除 `b`、
  创建 `c` 后目标精确为 `a/c` 且 ModRevision 相同。teardown 必须先用受限用户清理
  prefix，再由 root 禁用 auth，并删除 syncer role/user 与 root user；最后 AuthStatus
  必须 disabled、user list 必须为空，确保同一隔离实例可重复使用。

  真实验证在 `kubebrain-a228-auth` 临时 namespace 启动单副本
  `kubebrain:a221-empty-range-compare`，以 `--keyspace=a228-auth-mirror` 复用独立
  TiKV/PD 集群但与主数据面物理隔离。首轮约 2.66 秒、连续 5 轮约 13.1 秒、串行 race
  3 轮约 9.0 秒通过；加强初始/最终 auth 安全门后再次约 2.7 秒通过。一次把普通与 race
  并行运行会因两组测试竞争全局 auth 状态而让匿名 bootstrap 返回 `ErrUserEmpty`，改为
  符合 auth 状态机约束的串行执行后稳定通过，不记为服务端差异。

  compat module 全量 vet、server/etcd Auth/Range/Watch/Txn/Mutation/Leadership 相关
  回归 3 轮约 40.8 秒通过；隔离 Pod 与主三个副本均 Ready、零重启，隔离/主 keyspace
  的测试 prefix 均为零，主 endpoint proposal 健康。`make-mirror` 命令矩阵现明确包含
  双端鉴权契约；跨区域长期复制仍需独立 soak。本轮未发现服务端语义差异，运行镜像继续
  为 `kubebrain:a221-empty-range-compare`。
- **Compatibility A229 make-mirror revision replay and compacted failure（2026-07-18）**：
  继续对照 `client/v3/mirror/syncer.go` 与 etcdctl `makeMirror` 的 `--rev` 路径，新增
  历史重放和已丢失历史 fail-closed 差分。每个方向先在 mirror 启动前完成 seed Txn 和
  update Txn，再以 `--rev=<update revision>` 启动真实 CLI；目标必须只重放该 revision
  的 `a` 更新、`b` 删除和 `c` 创建，不复制更早的 `ignored` seed key，且 `a/c` 必须
  共享目标 ModRevision。

  compaction 分支连续写入 revision C 与 C+1，compact 到 C+1 后要求从 C 启动 mirror。
  reference 与 KubeBrain 都必须在 10 秒内非零退出，输出标准
  `etcdserver: mvcc: required revision has been compacted`，目标 prefix 必须保持为空。
  初版曾 compact 到 C 后也从 C 请求，CLI 持续等待至测试 deadline；这是 compact
  watermark 等值边界而非确定已丢失历史。最终门禁使用严格 `requested < compacted`
  的无歧义契约，不把等值行为误报为差异。

  真实验证在临时 `kubebrain-a229-mirror` namespace 使用
  `--keyspace=a229-mirror-revision`，完全隔离主实例 compact watermark。双向首轮约
  0.87 秒、连续 10 轮约 9.2 秒、race 5 轮约 5.8 秒通过；已有 Compact 与
  CompactRevisionBoundary 差分连续 3 轮约 1.1 秒通过。compat module 全量 vet、
  server/etcd Compact/Compaction/Watch/Range/Txn/Mutation/Leadership 相关回归 3 轮
  通过；双端测试 prefix 为零，隔离 Pod Ready/零重启，主 endpoint proposal 健康。
  etcdctl 矩阵现明确包含 `--rev` 历史重放与 compacted 错误。本轮未发现服务端语义
  差异，运行镜像继续为 `kubebrain:a221-empty-range-compare`。
- **Compatibility A230 client ordering across replica UID replacement（2026-07-18）**：
  对照上游 `TestDetectKvOrderViolation`、`TestDetectTxnOrderViolation`、
  `TestEndpointSwitchResolvesViolation` 与 `TestUnresolvableOrderViolation`，新增真实
  多副本 endpoint ordering 门禁。etcd 测试通过停机制造成员本地 MVCC 落后；KubeBrain
  副本无本地数据副本并共享 TiKV，因此可观察契约是 UID 重建后首次可服务的 serializable
  read 必须直接达到 durable revision，而不能先暴露 stale response 再由 client wrapper
  换 endpoint。

  测试为 `kubebrain-0/1/2` 创建三个临时、按 Pod name selector 固定的 NodePort Service，
  client 先只连接副本 0，并用 upstream `ordering.NewKV` 记录已见 revision。随后读取
  `kubebrain-2` 旧 UID、删除该 Pod；在其他副本连续提交 8 个双 key Txn 后，等待同名 Pod
  以新 UID Ready，再强制 ordering client 只连接副本 2。其 serializable Get 必须立即
  看到最后 value 且 header revision 不低于最后写 revision；同连接的 serializable
  multi-Get Txn 也必须满足该下界。最后依次固定到副本 1、0、2 读取，所有结果必须单调，
  order-violation callback 总数必须严格为 0。

  首轮及最终制品复检均约 7.6 秒、连续 5 轮约 41.3 秒、race 3 轮约 25.7 秒通过，
  共完成 10 次真实 Pod UID replacement；每次新容器 restart count 为 0。upstream
  ordering package 连续 10 轮、compat module 全量 vet、server/etcd 与 revision
  service 的 Serializable/
  ReadBarrier/Revision/Range/Txn/Mutation/Leadership 相关回归 3 轮通过。三个单副本
  endpoint 和主 Service endpoint proposal 均健康，测试 prefix 为零。本轮未发现新的
  服务端语义差异，运行镜像继续为 `kubebrain:a221-empty-range-compare`。
- **Operations A231 fail-closed metering completeness（2026-07-18）**：生产
  PrometheusRule 为 CPU、memory、RX、TX、PVC capacity/available 和备份
  artifact/timestamp 新增 8 个 source-count recording rule，并用预期的 9 个数据面
  容器、6 个 PVC、各 1 个备份源计算
  `kubebrain_dbaas:metering_data_complete`。8 条计量聚合现都以完整性为 1 作为门禁；
  任一源缺失时不再输出可能被账单系统误当成真实低用量的部分和，而是保留 source count、
  输出完整性 0，并在持续 15 分钟后触发 critical
  `KubeBrainMeteringDataIncomplete`。控制面必须将该区间标为不可计费并补采/人工对账，
  不得以零填充。manifest 测试固定全部 17 条 recording rule、门控 PromQL、实例标签和
  告警契约。
- **Operations A232 admission quota across replica replacement（2026-07-19）**：
  对照 etcd gRPC server 对一个 stream 生命周期占用一个并发 slot 的 admission 行为，
  新增真实三副本故障门禁。隔离滚动把
  `--max-requests-inflight` 从生产默认 1024 临时降到 1，并用两个按 Pod name 固定的
  NodePort 分别连接 victim 和 control 副本。Health Watch 在 victim 收到首个状态后持续
  占用唯一 slot；同 Pod Health Check 必须返回标准
  `ResourceExhausted: etcdserver: too many requests`，同时 control Pod 必须正常响应，
  明确该限额是副本本地容量而不是错误的进程间共享计数。

  测试随后记录 victim UID 并删除 Pod，要求旧 stream 在 30 秒内终止、同名 Pod 以新 UID
  Ready，且同一个固定 endpoint 上的首个 Health Check 立即成功，证明进程替换不会继承
  已死亡 stream 的 in-flight 计数。首轮约 10.0 秒、连续 5 轮约 46.8 秒、race 3 轮约
  29.5 秒通过，共完成 9 次真实 victim UID replacement。临时 Service 已删除，StatefulSet
  已恢复 `--max-requests-inflight=1024` 并完成三副本滚动；该门禁补齐取消释放单测之外的
  真实故障证据，不把 per-replica admission 误述为 DBaaS 全局租户配额。
- **Compatibility A233 lease switch across leader reload（2026-07-19）**：
  对照上游 `/root/etcd/tests/integration/v3_lease_test.go` 的
  `TestV3LeaseSwitch`，新增 24 轮 reference etcd/KubeBrain 差分和 32 轮真实 leader
  删除门禁。每轮把同一 key 从 lease A 并发切换到 lease B 并撤销 A，要求新 value 与
  lease B 绑定保留、A 的 TTL 为 -1，最终撤销 B 删除 key。一次 reference 对比连续
  10 轮约 35.7 秒通过。

  首次真实 leader 删除复现出继任者已发布 leader 身份、但 `ReloadLeases` 尚未完成的
  窗口；绑定到已存在 lease B 的 Put 会收到确定性的
  `NotFound: etcdserver: requested lease not found`，重试后才成功。现选主回调在发布
  leader 前调用 `PrepareLeaseReload`，撤销 gRPC health readiness，并关闭原子
  `leaseReady` 门；Grant/Revoke/KeepAlive、TTL/List 及所有带 lease 的 KV 写在完整
  durable snapshot 重载前统一返回可重试
  `Unavailable: etcdserver: lease state is reloading`。`ReloadLeases` 仅在加载、迁移
  和 sweeper 启动完成后开门，失去 leadership/停止 leases 时立即再次关闭。

  单元测试固定重载期间 Put/TTL/Grant 的错误码及重载后原 lease 可继续绑定，focused
  race 3 轮、server 相关包、全仓 test/vet 均通过。镜像
  `kubebrain:a233-lease-reload-ready` 滚动后 3 Pod Ready/零重启；连续三次删除当前
  leader（含一轮 race）分别约 8.2、10.1、6.4 秒通过。最终一轮在约 2.5 秒重载窗口
  实际观察到连续标准 `Unavailable` 后恢复，未再出现错误的 `LeaseNotFound`。
- **Compatibility A234 lease attachment recovery across replica replacement（2026-07-19）**：
  对照上游 `TestV3LeaseRevokeAndRecover`、
  `TestV3LeaseRecoverKeyWithDetachedLease` 和
  `TestV3LeaseRecoverKeyWithMultipleLease`，新增公开 client endpoint 上的真实 leader
  Pod UID replacement 门禁。故障前同时构造三类 durable 状态：已撤销 lease 及其已删
  key、从 lease 覆盖为 leaseless 的 detached key、依次从 lease A 重绑到 lease B 的
  key。删除当前 leader 后，测试等待继任者权威返回三个活 lease 与已撤销 lease
  `TTL=-1`，再逐项验证撤销 detached lease 不删除 leaseless key、撤销旧 lease A 不
  删除仍绑定 B 的 key、已撤销 key 不复活，且最终撤销 B 正常删除 key。

  使用运行镜像 `kubebrain:a233-lease-reload-ready` 连续轮换
  `kubebrain-1/2/0` 三个当前 leader，分别约 6.1、5.2、6.1 秒通过，其中第二轮启用
  race；每次均经历 Pod UID replacement，恢复窗口只见可重试的
  `Unavailable`/连接关闭。测试 prefix 清零，3 Pod Ready/零重启，公开 endpoint
  proposal 健康。compat vet、门禁无环境模式 10 轮、相关附件恢复 race 3 轮及全仓
  test 通过。本轮未发现新的服务端语义差异；新增证据把原有内部 attachment 单元保证
  提升为真实 TiKV/PD 共享数据面换主后的 client/v3 契约。
- **Compatibility A235 near-expiry lease promotion（2026-07-19）**：对照上游
  `TestV3LeasePromote`，新增短租约接近到期时的真实 leader replacement 门禁。公开
  client/v3 endpoint Grant 3 秒 lease 并绑定 key，等待 TTL 降至 1 秒后删除当前
  leader；继任者必须刷新并接管该 lease，故障恢复后 lease TTL 必须仍为正、key/value
  与 lease ID 必须保持，随后 lease 必须在新截止时间自然变为 TTL=-1 并删除 key。
  每次 TTL/Get 探测使用独立 300ms context，避免一次 follower proxy 内部重试吞掉整个
  断言窗口，同时不把 `Unavailable` 当作成功。

  在已提交镜像 `kubebrain:a233-lease-reload-ready` 上连续三次删除当前 leader，
  最终门禁分别约 8.3、8.4、7.4 秒通过，其中一轮启用 race；恢复窗口只见有界
  `DeadlineExceeded`，之后均实际观察到活 lease/key 和最终自然删除。测试 prefix
  清零，3 Pod Ready/零重启，endpoint proposal 健康；compat vet、无环境 race 10 轮
  及全仓 test/vet 通过。本轮审计曾尝试要求恢复后必须观察到 `TTL>=2`，但该值会被
  client 固定连接上的 follower proxy 恢复耗时消耗，且不属于上游测试契约；因此撤销
  相关服务端初始化重排，没有把测试传输时序误报成租约语义缺陷。
- **Compatibility A236 forwarded KeepAlive timeout contract（2026-07-19）**：
  对照上游 `TestV3LeaseKeepAliveForwardingCatchError` 的 forwarding timeout 与 client
  cancellation 分支，审计发现 follower 为每条 KeepAlive 消息创建到 leader 的 bidi
  stream 时只继承外部长连接 context。leader transport 已连接但不返回 response 时，
  `Recv()` 可无限挂住，既不满足 etcd 的 lease HTTP timeout，也不返回标准
  `Unavailable: etcdserver: request timed out`。

  现每条 follower-to-leader KeepAlive 转发具有独立 5 秒预算，建流、Send、Recv 任一
  阶段由该预算耗尽都映射为 `rpctypes.ErrGRPCTimeout`；若外部 client 先 cancel 或先到
  deadline，保留原始 `Canceled`/deadline 语义。代理自身预算不是 transport 断连，不再
  因本地 timeout 重置共享 leader client、连带中断其他 follower 请求。真实阻塞 gRPC
  Lease server 测试确认 100ms 注入预算稳定返回官方 timeout，client cancellation
  立即返回 `Canceled`，并覆盖统一三阶段错误映射；代理包全量普通 20 轮约 46.9 秒、
  race 10 轮约 25.5 秒通过。

  镜像 `kubebrain:a236-keepalive-forward-timeout` 滚动后 3 Pod Ready/零重启，公开
  endpoint proposal 健康。删除真实 leader 的 client/v3 Porcupine lease lifecycle
  race 历史约 8.0 秒通过，故障窗口记录 9 个可表达的不确定结果，Put/Get/KeepAlive/
  Revoke 整体仍可线性化。全仓 test/vet 通过。无 leader + `WithRequireLeader` 的
  `ErrGRPCNoLeader` 已由 A121 stream interceptor 覆盖，本轮不重复改变该契约。
- **Production A237 commit-exact KeepAlive timeout artifact（2026-07-19）**：
  A236 候选镜像包含 KeepAlive timeout 修复，但 OCI revision 仍指向构建前的 A235
  提交，且构建上下文包含用户未提交的 `go.mod`；该产物无法由 revision 精确重建，
  不满足 A206 已确立的生产 provenance 门禁。

  本轮先提交发布记录，再仅从最终提交的 `git archive HEAD` 构建
  `kubebrain:a237-provenance-exact`。构建前验证归档内 `go.mod` 与 Git blob 完全一致，
  且不携带工作区改动；构建后同时验证 OCI revision 和二进制 `version` 的 Git SHA
  等于同一完整提交 ID。精确镜像顺序滚动 3 个 KubeBrain Pod 后全部 Ready、零重启，
  endpoint proposal 健康；再次删除真实 leader 执行 race Porcupine lease lifecycle
  历史，覆盖 Put/Get/KeepAlive/Revoke 的故障窗口并保持可线性化。A236 的代理包
  普通/race 重复测试与全仓 test/vet 结果继续作为该精确源码归档的构建前质量证据。
- **Compatibility A238 nested Txn delete-interval duplicate key validation（2026-07-19）**：
  对照上游 `/root/etcd/tests/integration/v3_grpc_test.go:TestV3TxnDuplicateKeys`
  和 `server/etcdserver/api/v3rpc/key.go:checkIntervals` 补齐全部 10 项嵌套区间矩阵。
  新差分稳定发现 `Then(Put(k), Then(DeleteRange(containing k)))` 在 etcd 返回
  `InvalidArgument: etcdserver: duplicate key given in txn request`，KubeBrain 却成功执行。

  根因是本地 interval collector 在同一轮按请求顺序处理当前层 Put 和子事务，位于 Put
  后方的子事务删除区间无法反向检查已经收集的 Put。现与 etcd 一致分成两阶段：先递归
  汇总全部子事务 Then/Else 的 Put 与删除区间，再检查当前层全部 Put。父层 Put 与子事务
  DeleteRange 的前后两种顺序均拒绝，互斥 Then/Else 内同键 Put 仍合法，兄弟子事务重复
  Put 仍拒绝。聚焦单元矩阵连续 20 轮通过，全仓 test/vet 通过；修复前真实双端差分仅该
  项出现 `OK`/`InvalidArgument` 差异，其余 9 项一致。修复前完整双端 Differential
  253.469 秒通过既有全部组，并由本轮新增门禁暴露原覆盖空白。
- **Compatibility A239 Txn header revision snapshot contract（2026-07-19）**：
  对照上游 `/root/etcd/tests/integration/v3_grpc_test.go` 的 `TestV3TxnRevision` 与
  `TestV3TxnCmpHeaderRev`，补齐事务 response header 的双端门禁。顺序矩阵确认只读
  Range Txn 与删除不存在键的 Txn 均不推进 revision（相对 seed delta=0），单 Put Txn
  只推进一次（delta=1）。

  并发矩阵使用 500 个独立缺失键，同时启动 Put 与 `Version(key)==0` 的只读 compare
  Txn。若 Put revision 大于 Txn header，则 Txn 必须成功；若 Txn header 不小于 Put
  revision，则 Txn 必须失败，从而禁止 `Succeeded` 与 response header 描述不同 MVCC
  快照。真实 etcd 3.7 与三副本 TiKV-backed KubeBrain 均为零违规。该审计未发现新的
  服务端差异，新增测试作为后续 read barrier、compare guard 和 header 计算修改的持续
  P0 回归门禁。
- **Compatibility A240 concurrent Lease renew/TTL stress（2026-07-19）**：
  对照上游 `/root/etcd/tests/integration/v3_lease_test.go` 的
  `TestV3LeaseRenewStress{WithClusterClient}` 和
  `TestV3LeaseTimeToLiveStress{WithClusterClient}`，新增官方 client/v3 双端压力门禁。
  每端由 32 个 worker 各执行 8 轮 Grant(60s)、KeepAliveOnce、TimeToLive、Revoke，
  同时覆盖 leader 本地处理、三副本 Service 负载均衡与 follower proxy。

  一轮共 256 个完整 lease 生命周期；真实 etcd 3.7 与三副本 TiKV-backed KubeBrain
  均全部完成，KeepAlive TTL=0、意外 `ErrLeaseNotFound` 和其他错误计数均为 0。该审计
  未发现新的服务端差异，新增测试持续约束 `leaseCheckpointMu`、`leaseWriteMu`、
  `leaseMu` 的 renew/TTL/revoke 并发顺序，以及 follower forwarding 不得在压力下制造
  伪 lease-not-found。
- **Compatibility A241 direct-replica Lease read after revoke（2026-07-19）**：
  对照上游 `TestV3GetNonExistLease` 的逐成员读取方式，新增绕过 Service、分别直连三个
  KubeBrain Pod client endpoint 的黑盒门禁。每轮经 Service Grant 并绑定 key 后，每个
  Pod 必须在 Range 中返回相同非零 Lease，并由 `TimeToLive(Keys=true)` 返回正 TTL 和
  完整 attached key；Revoke 后先以每个 endpoint 的 linearizable Range 建立观察屏障，
  再要求 key absent、TTL=-1 且 keys 为空。

  真实三副本 TiKV-backed 部署连续 10 轮通过，覆盖 leader 本地读取和两个 follower 的
  proxy/read 路径。审计同时纠正旧文档中“follower 最新 Range 因 leader-only
  `keyLeaseIndex` 返回 Lease=0”的过时限制：value envelope v2 已持久化每个 MVCC 版本的
  lease ID，`kvToEtcdKv` 对新值直接读取 inline metadata；内存索引仅作为 legacy raw
  value fallback。本轮未发现新的服务端差异，该测试防止后续 envelope 或 follower read
  优化重新引入字段丢失和 revoke 后 stale TTL。
- **Compatibility A242 KV/Txn core-semantics graduation（2026-07-19）**：
  重新逐项审计当前矩阵、`/root/etcd/tests/integration/v3_grpc_test.go`、
  `v3_kv_test.go`、`clientv3/{kv,txn}_test.go` 与本仓库 113 个 compat 测试文件。
  Range/Put/DeleteRange 已覆盖 protobuf 边界、完整 sort/filter/limit 交互、历史与
  compact/future revision、empty/from-key、binary key、lease options、响应丢失
  at-most-once 和 follower 路径；Txn 已覆盖 compare 全矩阵、嵌套分支、范围 phantom
  guard、重复区间、写前错误验证、单 revision、ambiguous response 和并发 header
  snapshot。

  A238 新增上游重复区间矩阵时确实发现并修复最后一个已知差异；其后完整双端
  Differential 253.469 秒通过，A239-A241 又补齐并发 Txn header、Lease 压力和逐副本
  读取门禁，未产生新差异。因此矩阵将 KV 基础操作和 Txn 从“部分兼容”提升为“兼容核心
  语义”。这不是宣称复制 etcd Raft/bbolt 内部实现，也不关闭生成式输入、数天 soak、
  多点网络/磁盘故障和 TiKV 物理 PITR 等剩余生产验证；这些继续作为 P1/P2 门禁，不再
  被误写成已知客户端协议差异。
- **Operations A185 certificate rotation completion state（2026-07-18）**：新增
  `hack/production/validate-certificate-rotation.sh`，把 client/peer CA rollover
  收敛为 `begin -> overlap -> complete` 三阶段门禁。begin 固定全部 KubeBrain Pod
  name/UID/restart count/Ready 状态及旧、新证书 SHA-256，并要求旧凭据 endpoint
  proposal 成功；overlap 要求 Pod 快照不变且旧、新 client 凭据同时成功；complete
  要求新凭据成功、旧凭据失败，并在失败后立即再次用新凭据成功，防止把 endpoint
  outage 误记成旧证书已撤销。

  状态和 `kubebrain.certificate-rotation.receipt.v1` 都以 0600 临时文件、file
  `fsync`、不可覆盖 hard-link 和目录同步发布；receipt 绑定 instance、rotation ID、
  endpoint、replicas、旧/新证书指纹、Pod 未变、旧证书拒绝和完成时间。同参数重试会
  重做全部在线检查后复用原 receipt，参数漂移或证据冲突 fail closed。mock 测试覆盖
  完整生命周期、幂等 complete、阶段越级、overlap 新凭据失败、Pod UID 变化、旧凭据
  仍被接受、endpoint outage 误判和不安全 evidence 字段。

  门禁已接入 `hack/dev/tls-smoke.sh`。真实单节点 kind 上以独立 TLS namespace 完成
  CA overlap、叶证书热替换、撤旧、旧证书拒绝和新证书正向复检后签发 receipt；整个
  rollover Pod 零重启，长连接 watch/lease reconnect soak 持续完成 161 次写入。首次
  演练曾因 port-forward 退出把不可达误判为拒绝，新增正向复检后才重新通过，证明该
  fail-closed 分支由真实故障驱动。生产三副本 peer/auth+mTLS 热轮换能力已有 A25-A30
  smoke 证据；A185 本次在单节点环境验证的是平台完成契约，跨 AZ 仍需预生产重跑。
- **Operations A187 UID-fenced instance destruction（2026-07-18）**：生产
  KubeBrain/TidbCluster 清单补齐 `app.kubernetes.io/instance` 所有权标签，KubeBrain
  workload/service/PDB selector 同步绑定 instance，避免多实例 namespace 中仅靠 name
  误选。新增 `hack/production/destroy-instance.sh` 四阶段状态机：prepare 先用逻辑 v2
  completion gate 校验 backup prefix、最少记录数和最大年龄，记录 protected digest/
  revision，再冻结 KubeBrain、TidbCluster、PDB、Service、ServiceAccount 和每块
  PD/TiKV PVC 的 UID；quiesce 要求精确 `destroy:<instance>:<operation>` token 后缩容
  KubeBrain 到 0；destroy 删除前重查 UID 和未记录 PVC；complete 要求固定资源、PVC
  以及 PD/TiKV Pod/StatefulSet 全部消失后签发 `kubebrain.destroy.receipt.v1`。

  `hack/production/cmd/uid-delete` 使用 dynamic client 发送
  `DeleteOptions.preconditions.uid` 和 foreground propagation，关闭 `kubectl delete
  NAME` 在 get/delete 间名称复用的竞态。`uid-delete` 现在在空 kubeconfig 时优先使用
  in-cluster ServiceAccount，再回落到标准 kubeconfig；回归覆盖 in-cluster 优先和显式
  kubeconfig 不触发 in-cluster。状态、阶段 marker 和 receipt 均不可覆盖、
  file/directory fsync；中断重试接受 NotFound，但同名新 UID、额外匹配 PVC、存储
  workload residue 或 evidence 冲突均 fail closed。mock 测试覆盖完整生命周期和
  prepare/destroy/complete 重试、阶段越级、错误确认、备份失败、StatefulSet UID 漂移、
  新增 PVC 及 TiKV StatefulSet 残留；HTTP API 测试固定 DeleteOptions UID body 和
  Conflict 传播。

  真实 `kind-kubebrain-dbaas` 隔离演练从 `/dbaas/a187-destroy-proof/` 导出 1-record
  v2 artifact（revision `467764733078405143`，digest
  `3346940f8431c77c5b8ce823e32b022586ea2377b9801396c3d7d5f21b9eb3e7`），对两个临时
  namespace 中暂停 TidbCluster、2 PVC 和完整外围资源执行 prepare/quiesce/destroy/
  complete。12 次删除均由 API 接受 UID precondition，资源/PVC/workload residue 为
  0，complete 二次调用复用同一 receipt；测试 namespace 和 key 已清理，主 3
  KubeBrain、3 PD、3 TiKV 均 Ready 且零重启。首次 quiesce 因当前 kubectl 不支持
  `jsonpath={len(.items)}` 超时且未删除资源，计数改为稳定 `-o name | wc -l` 后才通过。
  namespace、TLS Secret、外部备份和账单记录保留给控制面按独立策略对账/清理。
- **Operations A188 Object-Lock backup lifecycle（2026-07-18）**：新增独立 Go
  module `hack/backup/objectstore` 和入口 `hack/backup/logical-object.sh`。upload 只接受
  通过 format/exact prefix/min records/max age gate 的 `kubebrain.logical.v2`，要求
  控制面提供固定绝对 `RETAIN_UNTIL_UNIX`，用 AWS SigV4 S3 `PutObject
  If-None-Match:*`、完整对象 SHA-256 checksum 和 COMPLIANCE/GOVERNANCE Object Lock
  发布；不存在先 HEAD 再无条件 PUT 的覆盖竞态。同 key 冲突只有在 size 与全部绑定
  metadata 一致时才视作可能重试，否则拒绝替换。

  Put 返回 version ID 后，工具重新下载该精确 version，使用生产 backup parser 验证
  format/digest/revision/records/leases，并通过 GetObjectRetention 核对 mode 和绝对
  retain-until，之后才以不可覆盖、file/directory fsync 方式签发
  `kubebrain.object-backup.receipt.v1`。receipt/object metadata 还绑定控制面稳定
  `OBJECT_STORE_ID`，避免不同 S3 账户的同 bucket/key/version 三元组被混淆。已有 receipt
  重试会重新下载和核对 retention，不创建新 version、不漂移保留期限。delete 必须匹配
  `delete:<instance>:<backup-id>`，再次核对 exact version、artifact digest 与 retention；
  未到期拒绝，到期只删 receipt 中 version，Head 确认不存在后签发
  `kubebrain.object-backup-deletion.receipt.v1`。删除后崩溃可幂等补证，保留期内 version
  提前消失则 fail closed。

  fake S3 测试覆盖 upload/delete 完整生命周期、条件冲突后的同内容重试、不同 artifact
  拒绝覆盖、远端 body 损坏不发 receipt、prefix/record/freshness gate、错误删除确认、
  retention 未到期、远端 retention 漂移、提前消失、到期删除和删除 receipt 重试；
  receipt 权限/不可覆盖也有独立测试。真实 MinIO 使用启用 versioning+Object Lock 的
  `kubebrain-logical` bucket：1-record artifact（revision
  `467764733078405143`，digest
  `3346940f8431c77c5b8ce823e32b022586ea2377b9801396c3d7d5f21b9eb3e7`）完成条件上传、
  精确 version 下载复核、同 operation 双次返回同 receipt；保留期内 delete 非零退出，
  到期删除及二次删除复用同一 deletion receipt。另以不同有效 artifact（revision
  `467764733078405145`）竞争同 key，被 `If-None-Match` 拒绝且未生成替换 receipt；
  最终 `mc ls --versions --recursive` 确认 bucket 无对象/version residue，测试 key 已清理。
- **Operations A243 HA-safe periodic backup policy（2026-07-19）**：新增 namespaced
  `KubeBrainBackupPolicy` CRD 和 `backup-scheduler`。策略以 UTC epoch 固定时间槽生成
  `backup-<policy>-<slot>`，只提交策略创建后的最新到期槽，不无界回填历史；双副本
  Deployment 无需 leader election，依靠确定性名称、Kubernetes Create 原子性和
  `operationqueue.Submit` immutable-spec 比对收敛到单一 Backup operation。

  参数模板保存在管理 namespace 的 Secret；scheduler 结构化解析 JSON，强制
  artifact、receipt 和 object key 包含 operation ID，占位展开后覆盖 backup ID、
  retain-until 和 scheduled time，再创建 operation 专属 immutable Secret。Operation
  spec 新增 Secret name/key 引用并继续绑定完整参数 SHA-256。worker RBAC 只增加
  Secret get；`operationctl parameters` 同时要求 Secret immutable、key 存在、base64
  有效且 digest 精确匹配，Backup executor 可在无本地参数文件时安全取回。模板漂移、
  同名 Secret 内容冲突和摘要漂移均 fail closed，手工参数文件流程保持兼容。

  单元测试覆盖两 scheduler 并发竞争只生成一个 operation/Secret、创建后首个合法槽、
  suspend、不回填和非唯一输出拒绝；queue 测试固定 immutable/digest 门禁，shell 测试
  覆盖 managed parameters 路径。生产 CRD/RBAC/Deployment 结构测试及 Kubernetes API
  server dry-run 已通过。该增量关闭“管理面定期策略触发”缺口；跨 namespace/region
  全局调度、跨账户保留规划和管理面长时间 HA soak 仍保留。
- **KV A244 RangeStream common-shape parity（2026-07-19）**：对照
  `/root/etcd/tests/integration/v3_grpc_test.go` 的 common Range/RangeStream 矩阵新增
  双端黑盒，补齐此前只覆盖 prefix 大范围、limit/count/keys-only 和二进制边界但遗漏的
  point hit/miss、`[k,k)`、反向区间、from-key 与历史 revision。首次对 reference etcd
  运行稳定复现三个差异：point stream 错误为空；反向区间因编码后的 MVCC scanner border
  失序而泄露一个历史值；历史数据虽正确固定在请求 revision，终态 header 却错误报告该
  历史 revision，而 etcd 报告请求开始时观察到的当前 store revision。

  `RangeStream` 现将天然至多一个 KV 的 point 请求以及空/反向区间路由到同语义 unary
  Range，再按流式 wire envelope 发送；递归范围仍走有界 partition scanner，不引入全量
  materialization。一般流在 revision/auth/read barrier 后独立固定当前 header revision，
  scanner 数据继续固定在请求 revision。直接 handler 测试逐字段比较 point/empty stream
  与 unary，并固定历史旧值 + 当前 header 的组合；双端测试同时保留 limit Count/More 和
  from-key 顺序，防止修复退化已有流式语义。
- **KV A245 RangeStream partial-compaction fencing（2026-07-19）**：继续对照
  `/root/etcd/tests/integration/v3_grpc_test.go::TestV3RangeStreamPartialThenCompacted`。
  KubeBrain scanner 原只在启动快照前检查一次 compact watermark；首块已发送后若另一请求
  把 compaction 推过固定快照，TiKV MVCC 仍可让已打开的历史快照读完，因此流错误以 OK
  结束。etcd 每块重新执行 revisioned Range，下一块会返回
  `OutOfRange/mvcc: required revision has been compacted`，使客户端丢弃已收到的部分结果。

  RangeStream 现从首个 backend chunk 固定 data revision，并在首块之后每个 gRPC wire
  chunk 发送前读取 fresh compact watermark；watermark 越过 data revision 立即以标准
  compacted status 终止。检查位于 wire split 内而非仅 backend batch 边界，因为一个
  1.5MiB scanner batch 仍可能按公开消息上限拆成多块。可控 Send 屏障单测稳定复现
  “首块→推进 revision→physical compact→继续发送”的旧 OK 结果并固定新错误。
  双端 live 测试只接受显式 disposable `KUBEBRAIN_COMPACTION_ENDPOINT`，拒绝共享主
  endpoint；真实验证使用独立 `--keyspace=a245-compact`，不推进生产实例 watermark。
- **KV A246 RangeStream upstream regression gates（2026-07-19）**：继续对照
  `/root/etcd/tests/integration/v3_grpc_test.go::TestV3RangeStreamWriteBetweenChunks`
  和 `TestV3RangeStreamLargeValues`。可控 `Send` 屏障在首块后提交一个仍位于请求范围内的
  新 key，确认最终结果、Count 和 header revision 仍固定在流启动快照；另把公开消息目标
  压到 256 bytes、写入 20 个 1 KiB value，确认不可拆分的单 KV 独占一块但不会令 chunker
  停滞或丢键。两项直接 handler 门禁连续 10 轮通过。

  本轮还以 reference etcd 对共享实例执行全部非 Compact `*Differential*` 黑盒（Compact
  继续只允许 disposable keyspace），覆盖 Alarm、binary key/mutation、Delete、HashKV、
  Lease、leasing 故障/重连、make-mirror、MemberList、namespace、Put、Range/RangeStream、
  serializable read、STM、Txn 与 Watch，共 261.657 秒全部通过；Watch/Lease 子集另连续
  3 轮通过。未发现新的客户端可观察差异，因此没有为制造提交而改动运行时路径；A246
  固化 upstream 2026 年新增回归面，防止后续 scanner/chunker 优化破坏已验证语义。
- **Runtime A247 startup-failure shutdown fencing（2026-07-19）**：A246 全量门禁与临时
  reference etcd 并行时真实触发 `12379/12380 address already in use`；旧
  `Endpoint.Run` 随即关闭 Badger，但 `brain.New` 用无法被 endpoint 取消的父 context
  偷启 Campaign，已获得 leadership 的 `EnsureEventLogStart` 在 storage close 后继续读，
  最终于 Badger skiplist `IncrRef` SIGSEGV，掩盖原始可操作的 bind 错误。

  Campaign 生命周期现由应用 `server` 明确持有：`NewServer` 创建专属 context/done，
  `Close` 幂等执行 cancel，等待 client-go `OnStoppedLeading` join 当前
  `onStartedLeading` 回调，再关闭 lease 和 peer/proxy 资源。Endpoint 在构造 server 前
  创建运行 context，并严格按“listener 退出→取消/等待后台服务→关闭 backend”清理；
  brain 协议层不再启动无主 goroutine。PeerService close 还会等待 etcd proxy 的
  leader-check loop，并关闭当前 clientv3/gRPC 连接，而非仅清 revision HTTP idle
  connections。可控 compaction-start 屏障证明 `Close` 不会先于 leadership callback
  返回，真实双端口占用测试固定返回 `address already in use`，proxy 测试固定 loop/client
  清理；三项普通测试与 race 各连续 20 轮通过，server/endpoint 全包通过。
- **Auth A248 Bearer-prefixed token parity（2026-07-19）**：对照 upstream
  `/root/etcd` commit `43a4c4ecd`。etcd 现允许 gRPC `token` metadata 使用 OAuth 风格的
  `Bearer <token>`；KubeBrain 原把整段值交给 simple/JWT verifier，因而稳定返回
  `InvalidArgument/etcdserver: invalid auth token`。鉴权入口现与 etcd 一样只精确剥离
  大小写敏感的 `"Bearer "` 前缀，裸 token 行为不变，`"bearer "` 仍 fail closed。

  request-local caller 继续保存原始 metadata 值，因此 follower 向 leader 转发时保持
  `Bearer` 凭据逐字不变，不会重复添加或降级为另一种凭据。单元回归同时覆盖裸 token、
  Bearer token、错误大小写和 outgoing metadata，定向普通测试连续 20 轮、race 连续
  20 轮及 server 全包通过。官方 client/v3 live 门禁在共享 TiKV 的独立
  `a248-auth` keyspace 启用 Auth 后直接发送 gRPC metadata，连续 3 轮确认裸 token 与
  Bearer token 成功，小写 bearer 稳定返回
  `Unauthenticated/etcdserver: invalid auth token`；未改动主实例 Auth 状态。
- **Read A249 leader-change read barrier fencing（2026-07-19）**：对照 upstream
  `/root/etcd` commit `9f83a29b3` 的 ReadIndex/leader-change race。KubeBrain follower
  原在请求前读取 leader address，收到 `/status` 成功响应后直接更新 backend read
  revision；若旧 leader 在 handler 已通过 `IsLeader` 检查后失去 leadership，旧响应与
  新 leader cache 更新同时到达，follower 可接受已失效 revision 并让线性读返回旧值。

  revision syncer 现于请求前固定 leader identity 和已观察的共享 election term，解析
  非零 revision 后重新读取 identity、term 与本节点 leader 状态；identity 变化、term
  变化或本节点已接任时均丢弃响应，以显式 `leader changed while fetching revision`
  进入既有有界重试。term 栅栏同时覆盖 A→B→A 回到同地址的 ABA 场景。双 HTTP leader
  可控测试修复前稳定把 backend revision 从 42 错设为旧 leader 的 1000，修复后改向
  新 leader 取得 2000；同地址 term 递增测试也固定必须发起第二次 fetch。identity 测试
  连续 100 轮、两类竞态 race 各连续 20 轮、revision 包和主仓全量测试通过。真实 kind
  三副本先在 rollout 中替换当前 leader，再由 upstream `ordering.NewKV` 直连三个副本
  并删除 `kubebrain-2` 触发第二次 Pod UID replacement；replacement 期间提交 8 个双键
  Txn，重建副本与全部 endpoint 最终均返回最新值且 revision 不低于写 revision，
  serializable/Txn ordering violation 为 0。
- **TLS A250 CRL checks on resumed sessions（2026-07-19）**：对照 upstream
  `/root/etcd` commits `2308ce157` 与 `e84205af4`。后者把 CRL hook 从
  `VerifyPeerCertificate` 改为 `VerifyConnection`，因为恢复握手同样必须重新读取动态
  CRL 并拒绝已撤销 leaf。审计确认 KubeBrain A30 已使用最终形态，但原测试每次都做完整
  握手，不能证明 callback 在 session resumption 上生效。

  新增 TLS 1.2 session cache 回归的双向矩阵：入站 mTLS client 与出站内部 client 均先
  完成首个 full handshake，再要求第二次 `ConnectionState.DidResume=true`；随后原地写入
  新 CRL，第三次尝试恢复同一 session 必须分别在 server/client 端返回 revoked serial。
  这也证明动态 `GetConfigForClient` clone 保持稳定 session ticket key，而不是因恢复实际
  未发生产生假阳性。两项普通测试各连续 50 轮、race 各连续 20 轮通过；现有实现已与
  upstream 最终修法一致，因此本增量只增加安全回归门禁，不改动运行时。
- **Auth A251 non-admin maintenance/member read compatibility（2026-07-19）**：对照
  upstream `/root/etcd` commits `114f6ad80` 与 `a2987fdee`，审计 Auth 开启后普通
  已认证用户访问 maintenance status、member list 与 alarm list 的行为，以及 alarm
  disarm 仍要求 root 的授权边界。KubeBrain 现有 handler 已采用相同边界，本增量把
  member list、alarm list 和 alarm disarm 纳入官方 client/v3 双端 Auth 差分结果，
  同时保留匿名请求、普通用户和 root 操作的精确 gRPC code/message 比较。

  使用全新独立数据分别启动 upstream etcd 与独立 keyspace KubeBrain Pod，完整 Auth
  生命周期 live 差分一次通过：匿名 member/alarm list 均被拒绝，普通已认证用户可读取
  member/alarm，普通用户 alarm disarm 被拒绝，且两端结果逐字段一致。服务端 member
  与 maintenance 授权单测普通模式连续 50 轮通过；race 模式连续 20 轮通过。本增量只
  扩大持续兼容门禁，不改动运行时。
- **Auth A252 leased Put/Txn attachment TOCTOU fencing（2026-07-19）**：对照
  upstream `/root/etcd` commit `70a2b4871` 对 Txn Put 的完整 `checkPutAuth` 修复。
  KubeBrain 已检查 `PrevKv` 读权限和目标 lease 上全部绑定键的写权限，但 Put/Txn 在
  入口授权后才获取 `leaseWriteMu`；有权限的并发写者可在两者之间给同一 lease 绑定普通
  用户无权访问的键，使后者用旧 attachment 快照通过 RBAC。

  leader 路径现保留入口检查以维持 follower 与错误优先级，并在取得共享 lease 写锁后
  对完整 Put/Txn（含嵌套 Txn Put）重新授权；锁持续持有到原子 TiKV 提交及 attachment
  index 更新完成，因此最终检查使用的键集合不可再变化。可控 leader admission 回归让
  请求通过首次鉴权后暂停，再向 lease 注入受保护键；普通 Put 与嵌套 Txn 修复前会继续
  写入，修复后均在触达存储前返回 `PermissionDenied`。定向普通测试连续 100 轮、
  race 连续 30 轮通过。全新 upstream etcd 与独立 TiKV keyspace 的官方 client/v3
  差分确认：普通用户虽可写目标 `/auth-allowed/`，仍不能把直接 Put 或 Txn Put 绑定到
  已承载 `/auth-protected/` 的 lease，且两端精确错误一致。
- **Watch A253 future-revision progress fencing（2026-07-19）**：对照 upstream
  `/root/etcd` commit `973847aa2`。KubeBrain 为历史 watch 把 `syncedRev` 初始化为
  `StartRevision-1`；当起点仍在未来时，显式 `RequestProgress` 和周期
  `ProgressNotify` 会把这个尚不存在的 revision 立即发给 client，错误宣称 watch 已经
  同步，可能使恢复逻辑跳过未来事件。

  每个 watch 现保存原始 start revision，progress snapshot 只包含 published watermark
  已到达起点的 watch；只要同 stream 仍有未来 watch，就禁止 stream-wide progress，
  per-watch fallback 只回应已具备资格的 watch。周期路径使用同一 published fence。
  created response 仍立即返回，不阻塞 watch 建立。确定性测试固定
  `published=5/start=10/synced=9`：起点前显式及周期 progress 均静默，published 推进到
  10 后才允许发送 revision 9。相关普通测试连续 50 轮、race 连续 20 轮通过。全新
  upstream etcd 与候选二进制的独立 TiKV keyspace 官方 client/v3 差分同样确认：
  `base+2` 起点前 150ms 无 progress，两个写推进后先收到目标事件，再收到合法 progress。
- **Watch A254 delivered-watermark progress fencing（2026-07-19）**：继续审计 A253
  发现 published watermark 与单个 watch 的 FIFO event stream 之间仍有窗口：全局
  published 已到 future start，但该 watch 的 event/progress marker 尚未被消费时，周期
  路径仍可能发送初始化的 `StartRevision-1`。这个响应虽然没有越过未来，却低于 client
  请求起点，且错误表示该 watch 已同步。

  progress snapshot 和周期通知现统一以 watch 自身的 delivered `syncedRev` 为资格：
  `startRevision > syncedRev` 时保持静默；只有同一 FIFO 中的事件或 in-band progress
  marker 到达 start 后才允许响应。资格判断不再读取可能先行的全局 published watermark。
  确定性测试固定 `published=5/start=10/synced=9`，分别确认 published 前静默、
  published 推进到 10 但 marker 未到仍静默，以及 marker=10 后才发送 revision 10；
  显式 RequestProgress 同样永不返回低于 start 的 header。相关普通测试连续 50 轮、
  race 连续 20 轮通过。真实三副本 A253 基线经 Service 命中 follower 后，官方
  client/v3 `WithProgressNotify` quiet watch 连续复现首个 progress 超时：follower 把
  from-now 请求改写为正数起点，却错误等待自身滞后的 published watermark；该复现作为
  A254 部署后同一黑盒的发布门禁。
- **Watch A255 follower from-now progress identity（2026-07-19）**：A254 三副本部署
  后，上述 quiet-watch 黑盒仍稳定超时，进一步证明问题不只是 follower 本地 published：
  为关闭 created response 与 backend 注册之间的事件缺口，follower 会把客户端原始
  `StartRevision=0` 改写成安全恢复点 `R+1`；A253/A254 随后丢失原始请求身份，把这个
  内部恢复点误当成客户端显式 future revision。没有后续写入时，leader 代理 watch 与
  follower 外层 watch 互相等待 progress，形成活性死锁。

  watcher 现分别保存 backend `StartRevision` 与客户端 `progressStartRevision`。普通及
  显式历史/future watch 两者相同；仅 follower from-now 注册保持 backend 从 `R+1`
  无缝回放，同时 progress floor 保留 0，允许外层立即报告已安全同步的 R。真正的
  future watch 仍要求自身 FIFO event/marker 达到起点。确定性测试制造
  `current > published`，确认重写请求以 `published+1` 注册、`syncedRev=published` 且
  `progressStartRevision=0`；与 future progress 组合测试普通连续 100 轮、race 连续
  20 轮通过。真实三副本 A254 基线的官方 client/v3 quiet-watch cadence 稳定超时；
  A255 部署后同一用例连续 10 轮通过，随后 client/v3 兼容模块全量、根模块全量及
  两处 vet 均通过。
- **Watch/Txn A256 structured lifecycle logs and compacted-range atomicity
  （2026-07-19）**：A255 运行态审计发现 Watch 正常关闭日志把 Go channel 直接作为
  structured field，klog 编码后产生 `<internal error: json: unsupported type: chan ...>`；
  这不是服务错误，却会污染审计并触发基于 `error` 文本的生产告警。WatcherHub 与
  backend Watch 的 8 个生命周期日志点现统一输出 `subscription=0x...` 稳定指针标识，
  不再暴露不可序列化对象；格式单测和相关 backend 普通连续 20 轮、race 连续 10 轮通过。

  同时对照 upstream `/root/etcd` commit `fbba4f46e` 的
  `tests/integration/txn_range_consistency_test.go` 补官方 client/v3 黑盒：先写两个
  revision 并 compact，再提交“读取已 compact 历史 revision，随后 Put 禁止键”的 Txn。
  KubeBrain 不依赖 Raft follower apply，但必须满足相同客户端不变量：Txn 返回精确
  `ErrCompacted`，且禁止键零落库。全新 upstream etcd 与真实三副本 TiKV-backed
  KubeBrain 各连续 10 轮通过；服务层已有
  `TestTxnRangeCompactedRevisionIsCheckedBeforeWrites` 固定提交前路径校验。
- **Auth/Txn A257 PrevKV and lease RBAC regression coverage（2026-07-19）**：
  对照 upstream `/root/etcd` commit `70a2b4871`。该修复要求 Txn 内 Put 复用完整
  Put 鉴权，而不能只检查目标键 WRITE 权限：`PrevKv=true` 还必须拥有目标键 READ
  权限，携带 lease 还必须能写该 lease 已关联的所有键，嵌套 Txn 和未选分支也必须在
  mutation 前完成相同检查。KubeBrain 的 `authorizeTxn` 已递归调用 `authorizePut`，
  lease 路径还在 `leaseWriteMu` 内二次鉴权，因此本轮未发现运行时实现差异。

  新增服务层 `TestAuthTxnPutPrevKVRequiresReadAndWrite`，用仅 WRITE 用户覆盖顶层和
  嵌套 Txn Put + PrevKV，均返回精确 `ErrPermissionDenied` 且旧值保持不变；连续
  20 轮通过。官方 client/v3 Auth 差分增加同一 write-only 场景，并与既有 leased
  Txn Put 场景共同固定 upstream 修复的两个分支；全新 upstream etcd 和真实独立
  `a257-auth-diff2` TiKV keyspace 的 KubeBrain 完整 Auth 生命周期结果一致。一次性
  Pod、Service、端口转发、参考 etcd 和临时目录均已清理。相关 Auth 服务层 race
  连续 10 轮、真实三副本 client/v3 兼容模块全量、根模块全量及两处 vet 均通过；
  并行争用同一 TiKV 时曾使一次根模块 backend 门禁失败，停止并行负载后 backend
  单独及根模块串行全量均通过，因此不把测试环境资源竞争误报为产品回归。
- **Range A258 KeysOnly + CountOnly precedence（2026-07-19）**：对照 upstream
  `/root/etcd` commit `e40f9c68e` 的
  `tests/integration/clientv3/TestKVGetKeysOnlyWithCountOnly`。两个标志同时设置时
  `CountOnly` 必须优先：response 保留当前 header 和精确 Count，但 `Kvs` 为空且
  `More=false`。KubeBrain 的无 revision filter Count 快路径直接返回无 payload count，
  其他路径的 `applyRangeOptions` 也先处理 CountOnly，因此本轮未发现运行时实现差异。

  新增服务层 `TestRangeCountOnlyTakesPrecedenceOverKeysOnly` 穿过 RPC Count 快路径，
  普通连续 30 轮和 race 连续 20 轮通过；新增官方 client/v3 黑盒在全新 upstream
  etcd 与真实三副本 KubeBrain 各连续 20 轮通过。生成式 Range option 差分新增
  `keys-and-count` mode，覆盖 2 个 revision、5 个 sort target、3 个 order、4 个
  limit、12 个 revision filter 与 4 个 payload mode，共 5,760 个双端请求形状，
  完整结果一致。根模块全量、真实三副本 client/v3 兼容模块全量及两处 vet 均通过；
  参考 etcd、端口和临时目录均已清理。
- **Operations A259 OIDC tenant-authorized submission API（2026-07-19）**：
  持久 operation queue 原只有 Kubernetes 原生提交身份，外部调用者无法安全映射到
  tenant/instance，终态 artifact 也无法证明请求者。Operation spec 现增加可选但不可变的
  `tenant` 与 `requestedBy`，幂等 Submit 将两者纳入精确 spec 比较；worker Claim 和
  Object Lock 终态审计 artifact 保留相同字段。内部 scheduler/CLI 的空值保持向后兼容，
  新外部 API 强制两者完整。

  新增 TLS-only `kubebrain-operation-api`：OIDC discovery/JWKS 验证强制 RS256、kid、
  issuer、audience、exp、sub、DNS tenant 和显式 instance claim；未知 kid 刷新 JWKS，
  缓存过期且刷新失败时 fail closed，非 loopback issuer/JWKS 禁止明文 HTTP。POST submit
  与 GET operation 都要求 token tenant 和 instance 授权，越权统一 404 防枚举；请求体
  限 64 KiB、拒绝未知 JSON 字段，响应不暴露参数 Secret 引用；参数引用仅允许
  `params-<tenant>-*` / `parameters.json`。ServiceAccount 仅拥有 namespaced operation
  create/get，不能 list、watch、读取 Secret、修改 status 或删除。
  Deployment 在 OIDC/TLS Secret 配置前保持 replicas=0 fail closed。

  OIDC key rotation、错误 audience/tenant/claim、过期缓存刷新失败、跨租户/实例访问、
  严格 body 和响应脱敏均有确定性测试；operation API/queue/audit race 连续 10 轮、
  production 包全量、manifest 单测和 Kubernetes server dry-run 均通过。管理 API 的
  多副本 HA、外部 IdP 故障 soak、全局 tenant inventory 与跨 namespace/region 调度仍是 P1。

- **Operations A260 OIDC refresh storm control（2026-07-19）**：管理 API 的 JWKS
  cache miss 现由进程内互斥合并；并发 unknown `kid` 只触发一次刷新，刷新失败和未知 key
  使用固定内存、可配置的 fail-closed 退避窗口，避免攻击者用无限 key ID 扩张缓存或在
  IdP 故障时制造 outbound 请求风暴。已缓存且未过期的合法 key 继续工作；缓存过期且刷新
  失败仍拒绝认证，不回退到 stale trust。确定性并发测试覆盖 64 caller 单次刷新、20 次
  故障请求只产生一次失败刷新及 IdP 恢复后的自动重试。Deployment 显式配置 5 分钟 TTL/
  5 秒退避，新增 zone/hostname topology spread，PDB 改为 `maxUnavailable: 1`，用于
  Secret 配置后扩为 3 副本。跨 Pod 刷新协调、真实外部 IdP 长时间故障 soak 与跨 region
  管理 API 仍是 P1。

- **Operations A261 explicit multi-namespace backup scheduling（2026-07-19）**：
  Backup scheduler 新增严格 DNS 校验、去重的 `--namespaces` allowlist；单个双副本
  Deployment 可按固定顺序协调多个 operation namespace，同名 policy 在不同 namespace
  生成彼此隔离的 Operation 和 immutable 参数 Secret。单 namespace list/template/policy
  失败以 `namespace/policy` 聚合返回，不阻塞其他 namespace 的到期提交。BackupPolicy
  新增强制 tenant，生成 Operation 固化 tenant 与 scheduler ServiceAccount 身份，供全局
  审计归属。

  RBAC 保留原 namespaced Role/RoleBinding 以支持无中断升级，另增加未全局绑定的可复用
  ClusterRole；每个额外受管 namespace 必须显式 RoleBinding 给中央 scheduler
  ServiceAccount，因此没有 cluster-wide Secret 或 Operation 权限。默认清单仍只授权
  `kubebrain-operations`。Deployment 新增 zone/hostname
  topology spread 和 `maxUnavailable: 1` PDB。测试覆盖双 namespace 同名策略隔离、单
  namespace 故障隔离、allowlist 非法/重复拒绝、RBAC/HA 清单及 API server dry-run。
  跨 Kubernetes cluster/region 调度、动态 namespace inventory 和全局容量感知仍是 P1。

- **Operations A262 managed namespace lifecycle RBAC（2026-07-19）**：补齐 A261
  暴露的执行/审批/归档权限边界。新增三个未绑定 ClusterRole，分别精确复制默认
  namespaced worker、approver、archiver 权限；目标 namespace 必须显式 RoleBinding 给
  `kubebrain-operations` 中的中央 ServiceAccount，不创建 ClusterRoleBinding。结构测试
  固定 worker 不能创建 Operation、approver 不能写 status/Secret、archiver 不能写
  status 或读 Secret，所有可复用角色自身不包含 subject。

  onboarding 文档现要求 scheduler、worker、approver、archiver 四类 namespace binding；
  teardown 要先 suspend policy、排空 Operation、完成 Object Lock 审计归档并释放 finalizer，
  再删除绑定和 namespace。真实 API Server 授权矩阵验证绑定 namespace 允许精确动作、
  未绑定 namespace 全拒绝。多 namespace worker/archiver Deployment 编排、动态 inventory
  与跨 cluster/region 生命周期仍是 P1。

- **Maintenance A263 leader-independent local diagnostics（2026-07-19）**：对照
  `/root/etcd/server/etcdserver/api/v3rpc/maintenance.go`，upstream `Status` 直接读取
  member 本地 raft/backend 状态，`Hash`/`HashKV` 直接读取本地 MVCC snapshot，三者都不
  提交 raft read request。KubeBrain 此前额外调用 `SyncReadRevision`，导致 leader
  不可达时 follower 的 endpoint status 和一致性 hash 也返回 Unavailable，妨碍故障诊断。

  现移除 Status 的 leader barrier；Hash/HashKV 因 KubeBrain 的 committed-revision cache
  是副本本地状态而 TiKV MVCC 数据为共享状态，在 leader 可达时尽力刷新 revision 以固定
  最新一致快照，但刷新失败不会中断成员本地诊断。HashKV 仍在 backend
  `logicalWriteMu` 内读取 revision、compact watermark 和 MVCC iterator 的同一快照，
  Status 无 leader时返回 `Leader=0` 和 `etcdserver: no leader` diagnostics。Alarm GET
  仍查询集群 alarm store，保留强制 barrier。确定性测试令 `SyncReadRevision` 失败，
  固定 Status/Hash/HashKV 成功而 Alarm GET 失败；真实三副本删除 leader 窗口继续验证
  幸存 follower 诊断可用。

- **Operations A264 fail-closed dynamic namespace inventory（2026-07-19）**：
  A261 的多 namespace scheduler 仍把 allowlist 固化在 Deployment args，租户 onboarding/
  teardown 每次都要求修改 PodTemplate 并滚动双副本，配置与逐 namespace RBAC binding
  也缺少明确发布顺序。

  scheduler 现可在每轮 reconcile 从中央 ConfigMap 的指定 data key 读取严格 JSON
  namespace 数组；超过 256 项、空数组、空值、重复、非法 DNS label、错误 JSON、ConfigMap/key 缺失
  均在任何 policy list 或 Operation submit 前令整轮 fail closed，绝不沿用进程内旧值。
  合法 inventory 排序后执行，更新无需重启。生产 ServiceAccount 只有对单一
  `kubebrain-backup-scheduler-inventory` resourceName 的 ConfigMap `get`，不能 list/watch
  namespace、读取其他 ConfigMap，目标 Secret/Operation 权限仍由未绑定 ClusterRole 加
  每 namespace RoleBinding 显式授予。默认 inventory 只含 `kubebrain-operations`。

  确定性测试使用同一 scheduler 实例将 inventory 从 tenant-a 动态扩为 tenant-a/tenant-b，
  固定两边 queue/Secret 隔离；六类损坏 inventory 均零提交。清单测试固定精确
  resourceNames RBAC、动态 flags、默认 JSON 和 ClusterRole 不含 ConfigMap 权限。生产
  runbook 明确 onboarding 先绑定四类身份再加入 inventory，teardown 先 suspend/排空/
  归档、移出 inventory 并等待 reconcile，再删除 binding。

- **Operations A265 inventory-aware cross-namespace worker claim（2026-07-19）**：
  A264 只让 scheduler 动态创建多 namespace Operation；类型专属 worker 仍把
  `OPERATION_NAMESPACE` 固定为单一队列，新增租户即使完成 scheduler/worker RoleBinding，
  任务仍会永久 Pending。直接建立持有所有 executor 凭据的“万能 worker”会扩大 blast
  radius，因此保留按 operation type 隔离的执行器，只扩展其 claim 底座。

  namespace inventory 解析已抽成 scheduler/worker 共用包，严格 JSON、DNS、重复和空数组
  规则只有一份。`kubebrain-operationctl claim` 可从同一 ConfigMap 读取 allowlist，按每个
  namespace 内匹配类型 Operation 的最近 `startedAtUnixNano` 排序，优先最久未服务队列，
  再调用原有 Queue claim，保留实例 Lease、审批、attempt、过期接管和 resourceVersion
  fencing。Claim 新增 namespace 字段；Backup、BackupDeletion、CertificateRotation、
  Destroy、PostRestoreAudit、RestoreCutover 六类脚本在 claim 后把 parameters/heartbeat/
  terminal update 固定到该 namespace。生产镜像新增 `/usr/local/bin/kubebrain-operationctl`；
  未显式传 kubeconfig 时优先使用 Pod ServiceAccount 的 in-cluster config，本地执行再回退
  标准 kubeconfig loading rules，避免非 root 容器错误读取 `/nonexistent/.kube/config`。

  worker 中央 Role 只增加指定 inventory ConfigMap 的 `get`，managed namespace
  ClusterRole 不增加 ConfigMap 权限。确定性测试固定同一 worker 第一次从 canonical
  tenant-a claim、第二次必须优先尚未服务的 tenant-b；共享 loader 普通/race 测试覆盖
  合法排序和六类损坏输入；Backup executor 黑盒固定后续 succeed 使用 claim 返回的
  namespace。动态 archiver 扫描/归档编排和具体 executor Deployment 模板仍是后续 P1。

- **Operations A266 inventory-aware HA operation audit archiver（2026-07-19）**：
  A265 后终态 Operation 的 Object Lock 归档仍需人工逐个指定 namespace/name 执行脚本；
  tenant 加入动态 inventory 后，未归档 finalizer 会永久阻止删除，而 archiver 没有可部署
  的扫描与重试进程。

  新增 `kubebrain-operation-archiver` 控制器，每轮先严格读取与 scheduler/worker 共用的
  namespace inventory；inventory 损坏时在列出任何 Operation 前 fail closed。合法
  namespace 分别列出 Operation，只选择仍持有 audit finalizer 的 Succeeded/Failed 终态，
  按 completed time、namespace、name 稳定排序并以跨轮轮转窗口限制每轮 batch，避免一个
  持续失败的最老对象饿死后续租户。单 namespace list 或单 operation 归档失败会聚合报告，
  但不阻止其他 namespace/operation 前进。

  processor 从当前终态对象生成规范化 artifact，object key 固定为
  `<prefix>/<namespace>/<operation-uid>.json`，retain-until 固定为
  `completedAtUnix + retention-duration`，因此多副本并发、进程崩溃和跨轮重试不会改变
  identity 或保留策略。上传仍调用 A198 的 Go Object Lock executor，保留
  If-None-Match、exact-version 下载、body/metadata/retention 复核和不可覆盖 receipt；
  只有验证 receipt 后才调用 A199 release。两个 archiver 同时处理同一 UID 时可复用同一
  exact version，resourceVersion 冲突的一方下一轮观察 finalizer 已释放，不需要持有
  worker Secret 或 Lease。

  生产镜像加入 archiver 与 logical-object 两个静态二进制；Deployment 具备只读根文件系统、
  临时文件限额、资源限额、zone/hostname spread、PDB 和独立 ServiceAccount。中央 Role
  仅可 get 指定 inventory ConfigMap，并 list/get/update Operation 主资源；目标 namespace
  ClusterRole 同样只新增 list，不授予 Secret、status、Lease、create 或 delete。模板默认
  replicas=0，必须先创建 `kubebrain-operation-archive-object-store` Secret、验证 Object
  Lock/versioning/retention，再显式扩为两个副本。确定性与 race 测试覆盖跨 namespace
  排序、invalid inventory 零访问、batch、失败隔离、稳定 key/retention、远端验证后
  finalizer release、过期 retention 拒绝和子进程环境覆盖；API Server dry-run 固定清单
  与 RBAC。

  真实 `kind-kubebrain-dbaas` 使用两个临时 namespace 和双副本 archiver 竞争处理两个
  Failed Backup Operation。MinIO 独立 bucket 已启用 versioning/Object Lock，两边分别
  产生一个 633-byte COMPLIANCE exact version，version ID 与 CR archive annotations
  一致，两个 finalizer 均释放；一个副本完成两项，另一副本完成一项并在第二项
  resourceVersion 冲突后安全重试，Pod 均零重启。ServiceAccount 对指定 inventory
  ConfigMap get=yes、ConfigMap list=no、目标 Secret get=no、Operation status
  update=no（使用 subresource SAR），而主资源 list/get/update 正常。inventory 改为
  `not-json` 后 one-shot reconcile 退出 1、archived=0，恢复默认 inventory 后临时
  namespace、Secret 和 Operation 均清理。具体六类 executor Deployment 模板仍是后续 P1。

- **Operations A267 type-isolated executor runtime（2026-07-19）**：
  A266 已能跨 namespace 调度、claim 和归档 Operation，但六类 executor 仍只有脚本，
  缺少可直接审计和部署的镜像入口、依赖及工作负载模板。新增通用
  `kubebrain-operation-worker` supervisor，在 SIGTERM 时终止当前子进程，并以独立成功/
  失败退避循环运行单一类型脚本。生产镜像固定包含 operationctl、逻辑备份/校验、审计
  probe、UID 删除工具、六类执行脚本及其 bash、jq、openssl、kubectl、etcdctl 运行时依赖。

  `kubebrain-operation-executors.yaml` 提供 Backup、BackupDeletion、RestoreCutover、
  PostRestoreAudit、CertificateRotation、Destroy 六个独立 Deployment。模板默认零副本，
  每类只挂载自己的 env Secret 和持久 workspace PVC，证书轮换另挂只读可执行 hook
  Secret；容器非 root、只读根文件系统、删除 capabilities、限制资源，并使用零中断滚动
  策略和 zone/hostname spread。所有执行脚本均先 claim，再从 Operation 绑定的 immutable
  parameters Secret 读取和校验摘要，不再要求控制面预先把某个参数文件注入长驻 Pod。

  这里的隔离边界是进程、环境 Secret 和 workspace 挂载隔离，不是独立 Kubernetes API
  身份：六类模板仍共用 operation-worker ServiceAccount，而该身份为跨 namespace 动态参数
  读取保留 Secret get。高合规租户必须把不同类型/租户放入独立 operation namespace 和
  ServiceAccount/RBAC 域，不能把本模板宣称为抵御已被攻陷 worker 的强多租户边界。
  启用前还必须提供支持多副本接管的 RWX 持久卷、完成参数路径/证书/hook 校验，并先以
  one-shot Operation 验证 claim、heartbeat fencing 和终态 receipt；具体跨账户/跨区域
  executor 身份隔离仍是后续 P1。

- **Operations A268 type-bound parameter broker（2026-07-19）**：
  A267 的六类进程虽只挂载各自 env Secret/PVC，但共用 worker ServiceAccount 为读取动态
  parameters Secret 仍拥有 namespace 级 Secret get；被攻陷的任一 executor 可绕过正常
  脚本读取同 namespace 其他类型凭据，并可尝试 claim 错误类型 Operation。

  对齐 etcd 在 `/root/etcd/server/auth/store.go` 的 `AuthInfoFromCtx` 后再执行
  `IsRangePermitted`，以及 `/root/etcd/server/etcdserver/api/v3rpc/watch.go` 对长连接请求
  持续做身份/范围授权的边界：参数内容也必须在已认证身份、授权类型和当前 fencing token
  同时成立后才可读取，不能把“持有 namespace token”等同于参数授权。

  新增独立 `kubebrain-operation-parameter-broker` HTTPS 服务。executor 使用 audience 固定为
  `kubebrain-operation-parameters` 的短期 projected ServiceAccount token；broker 通过
  Kubernetes TokenReview 验证 token audience 和精确 SA 名称，将六个 SA 固定映射到六种
  Operation type，并在读取 immutable Secret 前同时验证请求 namespace、当前 Running
  owner、attempt 和未过期 Lease。响应强制 `no-store`，错误身份、类型、owner、attempt、
  audience 或 Lease 一律不返回参数内容。只有 broker SA 在完成显式 RoleBinding 的 operation
  namespace 拥有 Operation/Secret get；六个 executor SA 的中央和 managed namespace Role
  均不含 Secret 权限。

  六类 Deployment 改用独立 ServiceAccount，并挂载 broker 专用 token 和只读 CA。
  fail-closed ValidatingAdmissionPolicy 在 status subresource 上把每种 Operation type 与
  对应 SA 用户名绑定，因此错误类型 SA 即使能 list Operation，也不能完成 claim CAS、
  heartbeat 或终态提交。broker、AdmissionPolicy 和 executor 模板都默认零副本或依赖显式
  TLS/CA 配置；单元/race 测试覆盖正确读取、跨类型拒绝、未知 SA、错误 audience、旧 owner/
  attempt 和过期 Lease。NetworkPolicy 将 broker 入口限制为六类 executor Pod；后续继续补
  跨集群 broker HA、证书在线轮换和可移植的 Kubernetes API egress 策略。

- **Compatibility A269 leasing range-contention gate isolation（2026-07-19）**：
  全量官方 client/v3 差分首次在 `nested-txn-delete` 上触发 45 秒超时。服务端调用链、
  leader 转发和日志复核确认没有范围屏障重入死锁；`leasing` 的范围写守卫会先读取
  range 最大 revision，再用后续 Txn 比较该 revision。零间隔 writer 因此把单节点
  reference etcd 与远程 TiKV 的绝对延迟差放大为无提交窗口，不是响应语义差异。

  差分场景现为每种 delete shape 使用独立 45 秒预算，错误路径先停止并回收 writer，
  且 writer 每轮保留 100ms 调度窗口。测试仍要求 writer 在删除期间实际完成操作，并
  对照普通 prefix delete、嵌套 Txn prefix delete 的响应类型、边界 key/owner 保留和
  删除后的 leasing cache/direct read 一致性。真实三副本 TiKV 数据面与 `/root/etcd`
  reference etcd 定向差分连续 10 轮通过；修改前的零间隔场景仍作为性能/饱和测试问题，
  不再混入语义发布门禁。

- **Operations A270 parameter broker online TLS leaf rotation（2026-07-19）**：
  A268 broker 原先由 `ListenAndServeTLS` 在启动时一次性加载 Secret，更新
  `tls.crt`/`tls.key` 后必须重启 Pod 才能生效。现新增通用原子证书 reloader：启动时
  fail closed，运行中每 30 秒校验完整 key pair、叶证书 NotBefore/NotAfter，成功后才
  替换 `GetCertificate` 指针；错误更新保留最后一份有效证书。`/readyz` 同时检查当前
  证书有效期，旧证书最终到期会摘流，`/healthz` 保留进程诊断能力。

  真实 HTTPS 握手测试在同一 listener 上依次看到 serial 1、serial 2，并证明损坏私钥
  reload 失败后仍呈现 serial 2；周期 reload、取消和错误回调均有确定性测试，20 轮 race、
  全量 production 脚本/控制器测试、manifest 测试和 vet 通过。该能力关闭同 CA 叶证书
  在线轮换缺口；CA 更换仍必须由控制面执行旧/新 CA 双信任窗口，不能只更新服务端 Secret。

- **Operations A271 operation API online TLS leaf rotation（2026-07-19）**：
  外部 OIDC operation API 是生产管理面的另一处 `ListenAndServeTLS` 静态加载点。现复用
  A270 原子证书 reloader，增加 30 秒轮询、`GetCertificate` 在线接管和证书有效期
  `/readyz`；无效 Secret 更新继续使用最后一份有效证书，证书过期则摘流但不杀死进程。
  Deployment readiness 已从 `/healthz` 切换到 `/readyz`，liveness 保持不变。

  operation API/OIDC、TLS reloader 与 manifest 功能测试连续 10 轮通过，operation API
  和 reloader race 连续 20 轮通过，vet 无告警。精确提交
  `a4a6657440586b12954a8085a44aefec9c14438f` 构建镜像
  `kubebrain:a271-operation-api-tls-reload`（image ID
  `sha256:9e62c910cd5a319c9d784f166badaea8aac00424a87cb67f27e83c59c28a97a7`）
  在 kind 双副本 API 上把证书 serial 3001 在线更新为 3002，两个 Pod 在 23 秒内收敛，
  UID 不变且零重启。真实 RS256 OIDC token 的 submit 返回 202，轮换前后 get 均返回
  200，两个 Pod 的 `/readyz` 均返回 204。smoke Operation 由 Backup executor 正常领取
  并进入 Failed 终态，archiver 写入 661-byte COMPLIANCE exact version、复核一年保留期
  后释放 audit finalizer，CR 正常删除。运行手册明确同 CA 叶证书逐 Pod serial/UID/重启
  验证，以及 CA 更新必须执行调用方双信任窗口。

- **Operations A272 parameter broker dependency-aware readiness（2026-07-19）**：
  parameter broker 原 `/readyz` 只检查 TLS 叶证书，即使 TokenReview 权限、Operation CRD
  或 Secret API 已不可用仍保持 Ready，Service 会继续把 executor 请求送入必然失败的
  Pod；client-go 请求也只继承外部连接 context，没有独立的 Kubernetes 依赖预算。对齐
  `/root/etcd/server/etcdserver/api/etcdhttp/health.go` 的有时限真实 API 检查，broker
  现以统一 5 秒 deadline 探测 TokenReview create、Operation get 和 Secret get，业务参数
  请求也使用同一 deadline。固定不存在对象和无效 token 不读取凭据；NotFound/未认证表示
  路径可用，transport、discovery、超时或 RBAC 错误 fail closed 返回 503，liveness 保持
  独立。

  focused 与 manifest 测试连续 10 轮、race 连续 20 轮、全量 production 测试和 vet 均
  通过。精确提交 `1db3dfb15d944f5488a7ff8951645aa3edebba20` 构建非 root 镜像
  `kubebrain:a272-broker-dependency-readiness`（image ID
  `sha256:6438825a3d6416da24775c4f0b54a5cf5d358ec77dd4aeb03a4750cb9715788d`）。
  kind 双副本初始 `/readyz`/`/healthz` 均为 204；撤销 TokenReview create 后第一次
  readiness 请求立即 503，两个 Pod 在 11 秒内均变为 NotReady，而 `/healthz` 仍为 204。
  恢复 RBAC 后两者重新 Ready/204，Pod UID 不变且零重启；无效 token 参数请求在 20ms 内
  返回 401。smoke 后 broker 恢复零副本并删除临时 TLS Secret。

- **Operations A273 operation API dependency-aware readiness（2026-07-19）**：
  外部 operation API 原 `/readyz` 在 A271 后仍只验证 TLS；Operation CRD/RBAC/API 已故障
  时继续接流，OIDC JWKS 网络失败又被折叠为用户 token invalid 401，Kubernetes RBAC
  错误则返回不可重试语义的 500。对齐
  `/root/etcd/server/etcdserver/api/etcdhttp/health.go` 的有 deadline 真实 API 检查，
  API 现以统一 5 秒预算探测固定 Operation GET，并严格区分对象 NotFound 与 CRD 路由
  NotFound。外部 OIDC 和 Operation 请求继承同一预算；JWKS provider 不可用/刷新退避、
  context deadline，以及 Kubernetes Forbidden/Unauthorized/timeout/503/429 均稳定返回
  503，真实无效身份仍为 401，liveness 不访问依赖。

  operation API/OIDC 与 manifest focused 连续 10 轮、race 连续 20 轮、全量 production
  测试和 vet 均通过。精确提交
  `c82dd16d44d1c9945dad3948cae76543b7e6425c` 构建非 root 镜像
  `kubebrain:a273-operation-api-dependency-readiness`（image ID
  `sha256:e19a3d04ffe81acccde62cd408d18a833d52bc4c6398c713d1cdeae1ae165d68`）。
  kind 双副本使用真实 HTTPS discovery/JWKS 与 RS256 token，健康时 readiness 204、
  liveness 200、授权 GET missing 404（约 23ms）。撤销 API Role 后 readiness 与同一
  OIDC GET 均立即 503，两个 Pod 在 12 秒内 NotReady，liveness 仍为 200；恢复 Role 后
  readiness 回到 204，两个 UID 不变且零重启。smoke 后 API 恢复零副本，临时 OIDC/TLS/
  CA 均删除。

- **Operations A274 bounded scheduler/archiver reconciliation（2026-07-19）**：
  backup scheduler 与 operation archiver 原先把进程生命周期 context 直接传给整轮
  reconcile；Kubernetes API 黑洞可永久占住 scheduler，archiver 的 Object Lock 子进程
  也只有 Pod 终止才会取消，HA 副本无法按 poll 周期重试。对齐
  `/root/etcd/server/etcdserver/api/etcdhttp/health.go` 以 request timeout 包围真实依赖
  调用的故障边界，新增共享 reconcile budget：父 context 取消立即传播，单轮 deadline
  到期取消所有 Kubernetes API 和 `CommandContext` 子进程，同时保留已完成计数与聚合错误。
  scheduler 生产预算为 2 分钟；archiver 为 15 分钟，覆盖最多 32 个串行 Object Lock
  操作。超时 archive 不生成 receipt、不释放 finalizer，下一轮仍使用稳定 identity 重试。

  budget、archiver processor 与 manifest focused 连续 20 轮、race 连续 20 轮、全量
  production 测试和 vet 均通过。精确提交
  `c4d663ef9a1cfa4e1ee82180d980383f131a0992` 构建非 root 镜像
  `kubebrain:a274-controller-reconcile-deadline`（image ID
  `sha256:8ce57c4b87e42fd3e0c4da0ebbd7f64e0d95cc875bc62479c455d5b121a4a45d`）。
  最终镜像中的 scheduler 与 archiver 分别连接故意不完成 TLS 握手的假 Kubernetes API，
  `--once --reconcile-timeout=2s` 均退出 1 并报告 `context deadline exceeded`，包含容器
  启动/销毁开销的端到端时间分别为 2729ms、2832ms。kind 双副本 scheduler 滚动到 A274
  后跨过至少一个 30 秒 poll，两个 Pod Ready、零重启且无 reconcile 错误；archiver 保持
  零副本并更新到 A274/15m 模板，三副本 KubeBrain 数据面不受影响。

- **Operations A275 isolated archive item timeout（2026-07-19）**：
  A274 的整轮 deadline 仍允许批次首个 Object Lock 上传占满全部 15 分钟预算，使后续
  终态 Operation 无法获得处理机会。archiver 新增 `--archive-timeout=2m` 单项预算，
  配置必须为正且不大于 `--reconcile-timeout`；单项失败或超时保留 audit finalizer，
  不生成 receipt，并在聚合错误的同时继续处理本批次后续候选。

  首次用真实最终二进制烟测时还发现 `exec.CommandContext` 只终止 executor shell：
  shell 派生的后代继承 `CombinedOutput` 管道后可继续存活，使 Go 等待管道 EOF 并越过
  单项 deadline。executor 现以独立进程组启动，取消时向整个进程组发送 `SIGKILL`，
  并优先返回 context deadline，封闭 shell 及其后代的资源泄漏边界。归档器 focused
  连续 20 轮、race 连续 20 轮、全量 production 测试（92.367 秒）和 vet 均通过。
  精确提交 `b7549df32f1a03c6002b6c20ada414b3d9abfc86` 构建非 root 镜像
  `kubebrain:a275-archive-item-timeout`（image ID
  `sha256:ca99e7bf7c963eac2636c8f9cd4a8d3ad0636bdbb1d1db44d275de946fdd0d09`）。

  最终镜像连接假 Kubernetes API 返回两个终态 Operation：首项 executor 派生
  `sleep 60`，次项立即退出 42。`--archive-timeout=1s --reconcile-timeout=5s
  --max-batch=2 --once` 在 1.710 秒退出 1，日志同时报告首项
  `context deadline exceeded` 与次项 `following-fast-failure`，证明完整进程组被回收且
  后续候选未被饿死。kind archiver 保持零副本并更新到 A275/2m 模板；三副本 KubeBrain、
  三 PD 和三 TiKV 均 Ready，主数据面零重启。

- **Operations A276 deadline-aware archive fairness（2026-07-19）**：
  A275 的数字轮转游标在选择批次时一次性前移 `max-batch`。当整轮 deadline 在首项耗尽，
  剩余候选仍会收到已取消 context，并被游标视为已经轮转；候选因成功归档从列表删除时，
  数字下标还会随列表收缩漂移。大量积压下，两种行为都会无谓延长未实际尝试对象的等待。
  archiver 现于每次处理前检查父 context，整轮取消后立即停止；游标改为最后实际尝试对象
  的 `completedAtUnix/namespace/name` 稳定身份，只在 processor 返回后推进。下一轮以
  `sort.Search` 从该身份之后恢复，身份已从列表消失时仍能选择正确后继，并在到达末尾后
  环回。单项 archive timeout 使用独立子 context，父 context 尚有效时仍继续本批次，
  保持 A275 的失败隔离语义。

  archiver focused 连续 50 轮、race 连续 30 轮、全量 production 测试（91.735 秒）和
  vet 均通过。精确提交 `fc7a8163f2c94cc6b6696a48f28e4a735454098c` 构建非 root
  镜像 `kubebrain:a276-archive-deadline-fairness`（image ID
  `sha256:38e57850327fd097231bfaf9c3144b47545e2078c2cb8bf0a1b87a1809fcbce8`）。
  最终镜像连接假 Kubernetes API 返回 `blocked/following/last` 三个终态 Operation，
  使用 `--archive-timeout=2s --reconcile-timeout=2s --max-batch=3` 连续运行：第一轮
  仅报告 `blocked` deadline；10 秒 poll 后第二轮先报告 `following` 与 `last` 的快速
  失败，最后才环回 `blocked` deadline。外部 16 秒观察窗退出 124，完整日志证明未尝试
  候选没有被整轮取消虚假推进。kind archiver 保持零副本并更新到 A276；三副本
  KubeBrain、三 PD 和三 TiKV 均 Ready，主数据面零重启。

- **Operations A277 bounded and resumable backup policy batches（2026-07-19）**：
  backup scheduler 虽有 A274 的 2 分钟整轮 deadline，但原实现没有 policy 批次上限，
  也没有跨轮游标；一个 API 调用耗尽 context 后仍会遍历其余 namespace 与 Policy，
  大规模 inventory 下既产生无效取消请求，也无法保证未尝试 Policy 的恢复顺序。
  scheduler 新增全局 `--max-policies=256`，先收集 inventory 中所有 Policy，再按
  `namespace/name` 稳定排序并截取批次。游标只在 policy reconciler 实际返回后推进；
  namespace list 或 policy 处理期间父 context 取消时立即停止，下一轮用 `sort.Search`
  从最后实际尝试身份之后恢复并在末尾环回。游标仅负责单 Pod 吞吐公平；HA 副本仍通过
  确定性 slot Operation ID、immutable Secret exact-content 校验和 AlreadyExists 收敛。

  scheduler/CLI/manifest focused 连续 30 轮、scheduler race 连续 30 轮、全量 production
  测试（91.948 秒）和 vet 均通过。精确提交
  `6f18fab39187481c88aa70f4a6691bdc83dcce47` 构建非 root 镜像
  `kubebrain:a277-backup-policy-fairness`（image ID
  `sha256:4543d0dc7ecdeca560d1c708e7c314b6c15be8219550a57ab7814dc82e246db7`）。
  最终镜像连接线程化假 Kubernetes API，inventory 返回 `a/b/c` 三个 Policy；
  `template-a` GET 永不响应，`template-b/c` 立即 404。使用
  `--reconcile-timeout=2s --max-policies=2 --poll-interval=10s` 连续运行时，第一轮
  15:03:56 只报告 `tenant-a/a` deadline，第二轮 15:04:06 只报告 `tenant-a/b` 与
  `tenant-a/c`，证明未尝试候选按身份恢复。kind 双副本 scheduler 滚动到 A277 并跨过
  一个 30 秒生产 poll，两个 Pod Ready、零重启且无 reconcile 错误；三副本 KubeBrain、
  三 PD 和三 TiKV 均 Ready，主数据面零重启。

- **Operations A278 worker executor process-group cancellation（2026-07-19）**：
  A267 的 `kubebrain-operation-worker` 使用 `exec.CommandContext` 运行六类 executor，
  SIGTERM/context 取消只向直接 shell 发送 kill。shell 派生的逻辑导出、kubectl、证书
  hook 或销毁命令可在 supervisor 和 Pod 主进程退出后继续运行，绕过本地停机边界并产生
  未受观察的副作用。A275 已在 archiver 的 `CombinedOutput` 场景证明同一根因，但该修复
  尚未覆盖 worker。

  新增共享 `internal/processgroup`：executor 以独立 process group 启动，取消时向负
  PGID 发送 `SIGKILL`，leader 已退出的 ESRCH 映射为 `os.ErrProcessDone`。operation
  archiver 改为复用同一实现并保留 pipe `WaitDelay`；operation worker 在连接 stdio 前
  应用同一配置。worker 回归测试让后台后代在 200ms 后写生存标记，取消后等待 300ms，
  连续 50 轮均确认标记不存在；worker/archiver race 连续 30 轮、全量 production 测试
  （90.794 秒）和 vet 均通过。

  精确提交 `ff2c674405d25c81af86e36c78e8ae552b401ee4` 构建非 root 镜像
  `kubebrain:a278-worker-process-group`（image ID
  `sha256:48c982206734f49475f3854f6ea90388b4477ee28a7cd27ffa3970da936e23ae`）。
  最终镜像容器中的测试 executor 先写 ready，再派生计划 3 秒后写 `survived` 的后台
  后代；ready 后向 worker 发送 TERM，容器 0 秒内以状态 0 退出并记录
  `executor failed: context canceled`。停止后等待 4 秒仍无 `survived`，证明真实镜像
  回收完整进程组。kind 中 Backup、BackupDeletion、RestoreCutover、PostRestoreAudit、
  CertificateRotation、Destroy 六个 Deployment 均保持零副本并更新到 A278；三副本
  KubeBrain、三 PD、三 TiKV 以及 A277 双副本 scheduler 均 Ready、零重启。

- **Operations A279 fail-closed cross-namespace claim errors（2026-07-19）**：
  `ClaimAcrossNamespaces` 原先会忽略某个 namespace 的队列检查错误并尝试健康队列；
  若最终没有成功 claim，又把检查/claim 错误格式化进以 `ErrNoOperation` 包装的字符串。
  `operationctl` 因而把 Kubernetes 超时、RBAC 拒绝和网络故障误判为退出码 3 的正常
  空闲；当后续队列有待办时还可能在无法证明“最久未服务”排序的情况下直接认领，隐藏
  全局公平性缺口。

  跨 namespace claim 现于每次队列检查和 claim 前检查父 context，取消后立即停止。
  检查阶段可聚合普通错误以提供 namespace 上下文，但只要任一检查失败就不进入 claim；
  claim 阶段按 `lastStarted/namespace` 排序，`ErrNoOperation` 才允许检查下一队列，
  首个其他错误立即 fail closed。原始错误使用 `%w`/`errors.Join` 保留，只有全部队列
  成功检查且均空闲时才返回 `ErrNoOperation`。

  operationqueue focused 连续 100 轮、race 连续 50 轮、全量 production 测试
  （91.819 秒）和 vet 均通过。精确提交
  `2cbf5d0b49f07f4a0ff0b25ae7ca6bc336c7a732` 构建非 root 镜像
  `kubebrain:a279-claim-fail-closed`（image ID
  `sha256:5dc9cf60ed1d50e2c5097cec287f68cfd59e4664c68b91641e128fe6fcc34ed8`）。
  最终镜像的 operationctl 连接线程化假 Kubernetes API：两个队列都为空时退出 3 且
  无输出；`tenant-a` list 返回 503、`tenant-b` 存在 Pending Backup 时退出 1，并保留
  `tenant-a: inspect queue: tenant-a API unavailable`。服务端记录的
  POST/PUT/PATCH/DELETE 总数为 0，证明故障路径没有 claim 副作用。kind 六类 executor
  Deployment 均保持零副本并更新到 A279；三副本 KubeBrain、三 PD、三 TiKV 和 A277
  双副本 scheduler 均 Ready、零重启。

- **Operations A280 bounded instance Lease compensation（2026-07-19）**：
  Queue claim 先取得 `coordination.k8s.io/Lease`，再 CAS 更新 Operation status。若 status
  请求冲突、超时或失败，原实现用同一个请求 context 释放 Lease，并忽略 release 错误；
  context 已取消时清理无法发出，留下没有 Operation owner 的孤立实例锁直到 TTL。类似地，
  Requeue/Finish 提交 status 后的 release 也使用原 context 且静默忽略失败。

  新增 5 秒 `leaseCleanupContext`，通过 `context.WithoutCancel` 保留 tracing/value，但
  与原请求取消信号解耦并重新施加严格 deadline。Claim status 冲突时只有补偿成功才继续
  候选；普通 status 错误与补偿错误通过 `errors.Join` 同时返回。Requeue/Finish 已提交
  status 后若释放失败，返回已提交对象和显式 cleanup 错误。释放继续先 GET 校验精确
  holder，再以 Lease UID precondition DELETE，避免迟到补偿删除新 holder。

  operationqueue focused 连续 100 轮、race 连续 50 轮、全量 production 测试
  （90.103 秒）和 vet 均通过。精确提交
  `b431207d6c304a4e72d8d92cf59013370b880a65` 构建非 root 镜像
  `kubebrain:a280-lease-cleanup`（image ID
  `sha256:6ff6cb97f260a50dbda2263153dd1b8119d700652b2a741708a078cad34dd1ed`）。
  最终 operationctl 连接线程化假 Kubernetes API：Lease POST 成功后，status PUT 保持
  35 秒，使 CLI 的 30 秒 context 到期。CLI 在 30.765 秒退出 1 并报告
  `context deadline exceeded`；服务端事件严格为
  `lease-created -> status-started -> cleanup-get -> cleanup-delete`，cleanup 在原 deadline
  后约 30ms 发出，最终 `lease_present=false`。kind 六类 executor Deployment 均保持
  零副本并更新到 A280；三副本 KubeBrain、三 PD、三 TiKV 和 A277 双副本 scheduler
  均 Ready、零重启。

- **Operations A281 committed Lease cleanup retry（2026-07-19）**：
  A280 会在 Requeue/Finish status 已提交但 Lease 清理失败时明确报错，不过调用方重试
  相同请求时，终态幂等分支和 Pending fencing 分支会直接返回，无法清除遗留 Lease。
  现在 Finish 仅在 phase、owner、attempt、receipt 和 message 全部精确匹配时重试清理；
  Requeue 仅在 Pending、owner 已清空、attempt 与 message 精确匹配时重试清理。两条路径
  都复用独立的 5 秒 cleanup budget。不同结果仍被 fencing/terminal guard 拒绝；Lease
  不存在或 holder 已替换视为旧 holder 已清理，替代 holder 不会被删除。

  operationqueue focused 连续 50 轮、全包连续 100 轮、race 连续 50 轮、全量 production
  测试（90.895 秒）和 vet 均通过。精确提交
  `27e58f14df5bdd259f5ee472e757e48eb91f4e0b` 构建非 root 镜像
  `kubebrain:a281-lease-cleanup-retry`（image ID
  `sha256:4ffc1899d1ac296714a8139cb87deb7344c93c44ef6726346d7813d264693db3`）。
  最终 operationctl 连接线程化假 Kubernetes API：第一次 Finish 只写入一次 Succeeded
  status，随后注入 Lease DELETE 503 并退出 1；第二次完全相同的 Finish 未再写 status，
  只执行 GET/DELETE 补偿并退出 0。服务端最终统计 `statusPuts=1`、
  `deleteAttempts=2`、`leasePresent=false`，事件顺序为
  `get-operation -> put-status -> get-lease -> delete-lease-failed -> get-operation ->
  get-lease -> delete-lease-succeeded`。kind 六类 executor Deployment 均保持零副本并
  更新到 A281；三副本 KubeBrain、三 PD、三 TiKV 和 A277 双副本 scheduler 均 Ready、
  零重启。

- **Operations A282 heartbeat commit reconciliation（2026-07-19）**：
  Heartbeat 原先先续长实例 Lease，再 CAS 更新 Operation status；status 冲突或失败会
  直接返回，已退出 worker 的 Lease 仍可能阻塞同实例一个完整租期。现在任何 status
  错误都会进入脱离父取消信号、最多 5 秒的 reconciliation：重读结果精确匹配 Running、
  owner、attempt 和目标 `leaseUntilUnix` 时，判定响应丢失但提交成功，返回重建 claim
  并保留 Lease；不匹配时按旧 holder 和 UID precondition 删除 Lease 后 fencing。重读
  本身失败时聚合原始 status 与 inspect 错误并保留 Lease，以 TTL 换取 uncertain commit
  下的单实例安全；清理失败同样显式聚合，替代 holder 不会被删除。

  Heartbeat focused 连续 50 轮、operationqueue 全包连续 100 轮、race 连续 50 轮、
  全量 production 测试（91.653 秒）和 vet 均通过。精确提交
  `3512edc2cdd9263b93d519b06bc472ed190ce009` 构建非 root 镜像
  `kubebrain:a282-heartbeat-reconcile`（image ID
  `sha256:d9d27fcb92fc46f63b63f50f269fdf981f8da91f22f2e33054f568c5735f3d03`）。
  最终 operationctl 连接线程化假 Kubernetes API：`backup-1` 的 Lease PUT 后 status PUT
  已持久化但返回 503，reconciliation GET 读到目标 deadline，CLI 退出 0、status PUT
  仅一次且 Lease 保留；`backup-2` 的 status PUT 返回 409 且未持久化，reconciliation
  读到旧 deadline，随后 GET/DELETE 精确 Lease 并以 fencing 退出 1。最终统计
  `statusPuts={backup-1:1,backup-2:1}`、`leaseDeletes=1`、`leaseA=true`、
  `leaseB=false`。kind 六类 executor Deployment 均保持零副本并更新到 A282；三副本
  KubeBrain、三 PD、三 TiKV 和 A277 双副本 scheduler 均 Ready、零重启。

- **Operations A283 instance Lease write reconciliation（2026-07-19）**：
  A282 关闭了 Heartbeat status 的不确定提交窗口，但实例 Lease 的 Create/Update 若已在
  API Server 持久化后丢失响应，仍会被当作失败：Claim 留下无 owner status 的孤立锁，
  Heartbeat 则使持有有效续租的 worker 退出。现在非 AlreadyExists 的 Create 错误和任意
  Update 错误都在独立 5 秒预算内 GET 同名 Lease，只有 holderIdentity、
  leaseDurationSeconds、acquireTime、renewTime 四项与本次写入全部精确匹配才按成功
  继续。字段漂移保留原写错误；GET 失败聚合 write/inspect 错误。父 context 已取消时
  仍可确认提交，并由后续 Claim status 补偿按 holder+UID 清理。

  Lease focused 连续 50 轮、operationqueue 全包连续 100 轮、race 连续 50 轮、全量
  production 测试（90.325 秒）和 vet 均通过。精确提交
  `871296ee9027c4ef3d8caaa60c1e7c93728faa6a` 构建非 root 镜像
  `kubebrain:a283-lease-write-reconcile`（image ID
  `sha256:e763191327e426425c8c29759343399cb6478b9d59f9c55886e7467953e5ee88`）。
  最终 operationctl 连续连接线程化假 Kubernetes API：Claim 的 Lease Create 已持久化
  后返回 503，GET 精确确认并成功提交 Running status；随后 Heartbeat 收到
  AlreadyExists，Lease Update 已持久化后返回 503，再次 GET 确认并成功更新 status。
  最终事件中 Create/Update 各一次、status PUT 两次、Lease GET 三次，
  `leasePresent=true`、Operation 保持 attempt 1 Running，没有重复认领或孤立锁。kind
  六类 executor Deployment 均保持零副本并更新到 A283；三副本 KubeBrain、三 PD、三
  TiKV 和 A277 双副本 scheduler 均 Ready、零重启。

- **Operations A284 status transition reconciliation（2026-07-19）**：
  A281 允许调用方在 Requeue/Finish status 已提交但 Lease 清理失败后用相同请求补偿，
  但 status PUT 本身已提交、响应丢失时，单次 operationctl 仍会误报失败并留下 Lease。
  现在两条转换在任意 status 错误后使用独立 5 秒预算线性化 GET：Requeue 精确匹配
  Pending、空 owner、原 attempt/message 和零 lease deadline；Finish 精确匹配目标终态、
  原 owner/attempt、receipt/message、零 lease deadline 和正数 completion time。确认
  已提交时清理 Lease 并按成功返回；确认未提交时也清理退出 worker 的旧 holder，再保留
  原写错误，冲突映射为 fencing；inspect 失败则聚合错误并保留 Lease。后续幂等重试也
  使用相同系统字段约束，部分或畸形 status 不可伪装成完成。

  transition focused 连续 50 轮、operationqueue 全包连续 100 轮、race 连续 50 轮、
  全量 production 测试（91.474 秒）和 vet 均通过。精确提交
  `629c4353362f33fadaf3a6aa464c7f87a8403512` 构建非 root 镜像
  `kubebrain:a284-status-transition-reconcile`（image ID
  `sha256:331fa088c5882187318b966ad867211e47b6d08bea291e09c73d0cd746d91ff6`）。
  最终 operationctl 对两个已认领 Operation 分别执行 Finish/Requeue；假 Kubernetes API
  均持久化 status PUT 后返回 503。两次单调用均 GET 确认、GET/DELETE 精确 Lease 并
  退出 0；最终每个 status PUT 恰好一次、`leaseDeletes=2`、`leaseCount=0`，状态分别为
  Succeeded 和 Pending，Pending 保留 attempt 1 且 owner 为空。kind 六类 executor
  Deployment 保持零副本并更新到 A284，PD/TiKV 3+3 和 A277 scheduler 2/2 Ready。

  更新后的健康检查发现长期运行的 `kubebrain-2` 本地 `/ready` 返回
  `RAFT NO LEADER`，成员表仍有完整三成员，`kubebrain-0/1` 可服务且 `kubebrain-1`
  已成为 leader；120 秒内未自动恢复。重建该无状态 Pod 后 1.571 秒 Ready，并重新连接
  `kubebrain-1`，主 KubeBrain 恢复 3/3 Ready、零容器重启。该本地 leader view 长期失效
  的自动恢复仍是后续可靠性差距，不能以本次人工 Pod 重建视为已关闭。

- **Follower proxy A285 same-process leadership recovery（2026-07-19）**：
  A284 观察到的长期 `RAFT NO LEADER` 根因是 etcd proxy 的本地状态机：副本成为 leader
  时会关闭 forwarding client，却保留此前 follower 阶段的 `curLeader`。若该进程后来
  失去领导权，而 successor 正好是这个旧地址，same-leader 快路径因字符串相等直接返回，
  即使 client 已为 nil，之后每秒循环也永不重新拨号。现在成为 leader 时同时清空
  `curLeader` 和旧错误；same-leader 快路径还必须要求非 nil client。回归测试覆盖
  follower→leader→demotion→同一旧 leader 的完整生命周期，以及 nil client 配旧身份
  的防御状态。

  调试同时关闭 LeaseKeepAlive 内部 deadline 的竞态：gRPC 可能先返回
  `codes.DeadlineExceeded`，本地 `callCtx.Err()` 稍后才可见；父请求仍存活时两种信号都
  统一映射为 etcd timeout。相关生命周期/超时 focused 连续 200 轮、focused race 50 轮、
  etcdproxy 全包连续 50 轮、全包 race 20 轮、server 全量、`pkg/...` 全量（backend
  42.236 秒、etcd 34.726 秒）和 vet 均通过。精确提交
  `9ef79182feb3482f502d5c2c64fee4b34dc0c1a9` 构建非 root 镜像
  `kubebrain:a285-proxy-leadership-recovery`（image ID
  `sha256:81936469bdfb6537ac37c0f5a0bc3ebd299246cef178d1293507a1a5e2085573`）。

  kind 主 StatefulSet 在 17.330 秒内滚动到 A285。第一次暂停 `kubebrain-2` 12 秒触发
  liveness 容器重启，因此仅作为测试窗口校准，不计入同进程恢复证据。第二次选择此前已
  作为 follower 连接 `kubebrain-1`、随后成为 leader 的 `kubebrain-0`：暂停 PID 9 秒后，
  `kubebrain-1` 在 term 410 接任；恢复原 PID 后日志记录 `stopped leading`、同进程
  election retry，并约 1 秒后 `conn to new leader` 指向 `kubebrain-1`。`kubebrain-0`
  Pod UID 始终为 `eea0a338-2987-4bfd-aa92-ab2e2d93acf5`、restartCount 0，三端
  `/ready` 均返回 200，endpoint status 一致报告 leader `kubebrain-1`、term 410。
  PD/TiKV 3+3 Ready；六类零副本 executor 模板也更新到 A285。A284 的本地 proxy
  leader view 长期失效差距由此关闭。

- **Operations A286 management write reconciliation（2026-07-19）**：
  Operation Submit、Approve 和 UID-fenced Delete 过去把 Kubernetes API 的非成功响应
  直接视为未提交，无法区分服务端已持久化但响应丢失。现在三条路径都在独立 5 秒预算内
  线性化回读：Submit 仅接受 immutable spec 完全一致且仍有 audit finalizer 的对象；
  Approve 仅接受原 UID 上完全一致的 approver/approval ID，同名替换 fail closed；
  Delete 仅在 NotFound 或不同 UID 时确认旧目标已经消失，并保留替代对象。回读本身失败
  时聚合原写错误和 inspect 错误。

  回归测试确定性注入 Create/Update/Delete 已提交后丢失响应、spec/finalizer 漂移、
  approval inspect 故障、replacement UID 及原删除目标仍存在。管理写 focused 连续
  100 轮、focused race 25 轮、operationqueue 全包连续 100 轮、production 全量
  92.449 秒和 `go vet ./hack/production/...` 均通过。代码提交
  `dad352a2504b3ae128adb8a5d12b51dd1bf4600a`；同时提交
  `3fa46c7dfdd3d83e664a6394155e72eb16461302` 修正 `operationctl --help` 遗漏的
  `approve` action。由后一精确提交构建非 root TiKV 镜像
  `kubebrain:a286-management-write-reconcile`（image ID
  `sha256:6550f1fe3a27d77e6aa2b5691adee1e4d93cc96cc7a9005785aa7b786e6fdee7`）。

  镜像内 `operationctl` 对 kind API 完成真实 Destroy Submit；管理员身份审批被
  ValidatingAdmissionPolicy 正确拒绝，切换到专用 approver ServiceAccount 后 Approve
  成功并写入不可变证据。测试对象随后由 Destroy executor 身份合法迁移到终态，补齐归档
  证据后释放 finalizer 并删除。六类 executor Deployment 保持零副本并更新到 A286；
  主 KubeBrain 3/3 Ready（A285）、PD/TiKV 3+3 Ready、A277 scheduler 2/2 Ready。
  audit finalizer release 的 Update 不确定提交仍是独立后续差距，本阶段不声称关闭。

- **Operations A287 audit finalizer release reconciliation（2026-07-19）**：
  A286 保留的 release Update 不确定提交已关闭。`operationauditrelease.Release` 在 Update
  返回错误后使用独立 5 秒预算回读；只有原 UID、audit finalizer 已移除及 receipt SHA、
  artifact SHA、object version 三项 annotation 全部精确匹配时确认提交。NotFound 表示
  finalizer 已释放且对象已完成删除；replacement UID 和 inspect 失败均 fail closed，后者
  聚合原写与回读错误。构造更新对象时还复制 annotation map，避免修改首次 GET 快照。

  单包连续 200 轮、race 50 轮、archiver/CLI 集成面连续 50 轮、production 全量
  91.910 秒及 `go vet ./hack/production/...` 均通过。可靠性提交
  `6797694c079cc0f245007149b73653bd697a2119`。镜像审计发现 Dockerfile 未发布已有的
  `cmd/operation-audit`，提交 `daee1648cc6884e2c64e15e997bda87ece7908b7` 同时构建并复制
  该非 root CLI。由后一精确提交构建 TiKV 镜像
  `kubebrain:a287-audit-release-reconcile`（image ID
  `sha256:c61c5ada21bf870206a5cbe687b5d02b40633aae043d96b2a24601ab1ad722f3`）。

  kind 真实 smoke 由 Backup executor 身份把 Operation 合法推进到 Succeeded；镜像内
  `kubebrain-operation-audit` capture 得到 745 字节 canonical artifact（SHA-256
  `a4647f5e784fa0066fe2bfd9ac049fcdc6f2c4f73c7fb5bee9b9962d6606d02f`），再由专用
  archiver 身份 release。结果仅移除 audit finalizer，保留 `example.com/a287-smoke`，
  三项 archive annotation 精确匹配；同证据重试前后 resourceVersion 均为 `644714`。
  测试对象按门禁清理。archiver 和六类零副本 executor 模板更新到 A287；主 KubeBrain
  3/3、PD/TiKV 3+3、A277 scheduler 2/2 当前均 Ready。

- **Operations A288 deterministic cross-process audit receipt recovery（2026-07-19）**：
  审计 A287 后发现，archiver 每次 Process 都使用新的临时 receipt 目录；远端 exact
  version 已存在但本地 receipt 丢失时，objectstore 以本次重试的 `Now` 填充
  `ArchivedAtUnix`。因此同一不可变 version 在 Pod 重启或两个 archiver 竞争时会产生不同
  receipt SHA，既不满足跨进程幂等，也可能让竞争的 finalizer release 使用不同证据。

  现在上传成功或条件冲突恢复取得 version ID 后，统一 Head 精确 version，复核 metadata、
  size 和返回的 version ID，并以远端 `LastModified` 固化 `ArchivedAtUnix`。时间戳缺失、
  非正数或不早于 retain-until 时 fail closed 且不发布 receipt。测试使用全新 receipt
  路径并把重试时钟推进一分钟，仍要求 receipt 完全相同；同时覆盖缺失远端时间戳。
  focused 连续 200 轮、focused race 50 轮、objectstore 全包连续 50 轮、全包 race
  20 轮、archiver/audit 集成面连续 100 轮、production 全量 91.887 秒及两侧 vet 均通过。
  代码提交 `59fecd3cc308a6dabfd5569f773ae64093b633b9`。

  由该精确提交构建非 root TiKV 镜像 `kubebrain:a288-stable-audit-receipt`（image ID
  `sha256:f1061f11b680197a25b972c9a077616668251295b0d5da1f7d03c98c861a7741`）。
  真实 MinIO `kubebrain-logical/operation-audit-a288/a288-receipt-smoke.json` 首次归档
  version `65c34a04-8a1a-488e-a32d-d1e06055a78e`；相隔 2 秒的第二个独立容器使用全新
  本地 receipt 路径，从远端恢复后仍得到 `archived_at_unix=1784502112`，两份 receipt
  SHA-256 均为 `0128eff09b1a6c869153bf0d293b33f323c46ebc756c8a80ccf04bca7d3308d5`
  且逐字节相同。该 COMPLIANCE version 保留到 `1784505711`，到期后必须按 exact version
  清理。archiver 与六类零副本 executor 模板更新到 A288；主 KubeBrain 3/3、PD/TiKV
  3+3、A277 scheduler 2/2 当前均 Ready。

- **Backup A289 deterministic cross-process upload receipt recovery（2026-07-19）**：
  A288 的时间不稳定同样存在于通用逻辑备份 Upload：本地 receipt 随 Pod 消失后，条件
  冲突恢复会用本次重试 `Now` 生成 `UploadedAtUnix`，使同一 backup exact version 的
  receipt SHA 变化。该 SHA 会进入 Backup operation 成功状态、后续 BackupDeletion 参数
  和 inventory manifest，因而不是纯展示字段。

  backup 与 audit 现共用 exact-version Head 校验：复核 metadata、size、返回的 version
  ID，并以远端 `LastModified` 固化 upload/archive 时间。缺失、非正数或不早于
  retain-until 的时间戳都不发布 receipt。测试用新 receipt 路径和推进后的请求时钟验证
  跨进程恢复逐字段相同，并固定缺时间戳 fail closed。focused 连续 200 轮、focused race
  50 轮、objectstore 全包连续 50 轮、全包 race 20 轮、production 备份/清单/删除/归档
  选择性连续 3 轮 88.452 秒、production 全量 91.644 秒和两侧 vet 均通过。代码提交
  `88af66382a706b75068553542149eed658aa1ddf`。

  精确提交构建非 root TiKV 镜像 `kubebrain:a289-stable-backup-receipt`（image ID
  `sha256:d799a4ef88a0f3863f950ff29a9b39fe306295f8742fa3c175ef335945150d4d`）。
  真实 KubeBrain `/registry` 在 revision `467796068338761731` 导出 58 条记录、0 lease，
  artifact SHA-256 为
  `48eb2a4c2754fd49364a639d3d3df4652d21154dae811b276c36db6f3fabf27a`。MinIO
  `kubebrain-logical/backup-a289/a289-stable-receipt.jsonl` version
  `d09262ba-aa03-4680-a2e7-3292b9f9eec1` 由第二个独立容器用全新 receipt 路径恢复后，
  `uploaded_at_unix` 仍为 `1784503285`；两份 canonical receipt SHA-256 均为
  `0b7ecb9f07898699dab174381fe9da200b6367cc7e6eaee3b2179e8721fa32a5` 且逐字节相同。
  COMPLIANCE version 保留到 `1784506884`，到期后按 exact version 清理。archiver 与六类
  零副本 executor 模板更新到 A289。

- **Backup A290 deterministic cross-process deletion receipt recovery（2026-07-19）**：
  exact-version 删除成功后，S3 不提供可恢复的物理删除时间；旧实现把本次调用的 `Now`
  写入 `deleted_at_unix`，导致本地 receipt 随 Pod 丢失后，同一已删除 version 的重建
  receipt SHA 变化。该 SHA 会进入 BackupDeletion operation 完成证据和不可变审计归档，
  因而必须跨进程稳定。

  删除许可仍使用当前时钟并重新核对远端 retention；到期前拒绝，保留期内 version 提前
  消失仍 fail closed。成功删除或到期后恢复时都以 exact-version Head NotFound 证明当前
  不存在，并把 `deleted_at_unix` 固定为 `retain_until_unix`，语义是最早合法删除边界；
  实际工作流完成时间由上层 operation receipt 承担。校验器和生产 executor 同步要求两者
  精确相等。测试覆盖同路径重试、全新 receipt 路径和推进后的请求时钟；focused 连续
  200 轮、focused race 50 轮、objectstore 全包连续 50 轮、全包 race 20 轮、
  BackupDeletion 选择性连续 3 轮 51.825 秒、production 全量 91.166 秒以及两侧 vet
  均通过。代码提交 `65b10ed212680e86847d30a3ae2dbb18ca13f5e6`。

  精确提交构建非 root TiKV 镜像 `kubebrain:a290-stable-deletion-receipt`（image ID
  `sha256:28d0461e1fa046dea9f6af75a29cd6f646794111820265c2e3988e0d55b3e81c`）。
  真实 KubeBrain `/registry` 在 revision `467796068338761731` 导出 58 条记录、0 lease，
  artifact SHA-256 为
  `95ddc319c1d3288eb3153269fe439ce8199aa94c85fe3f12db039acf8163c389`。真实 MinIO
  GOVERNANCE version `8626e1dd-21f0-42ff-b94d-11fe2eb8ecca` 保留到 `1784504294`：
  到期前删除被拒绝且不生成 receipt；到期后首次删除与两秒后的独立容器、全新路径恢复
  得到逐字节相同的 canonical receipt，SHA-256 均为
  `35650ecf927e27fe24eeeeb42e29f57c680fca4d92ffb914b7da76a2ee57d93c`，且 version
  已清理。archiver 与六类零副本 executor 模板更新到 A290；A288/A289 的 COMPLIANCE
  验证 version 继续保留到各自期限，到期后按 exact version 清理。

- **Backup A291 uncertain exact-version deletion reconciliation（2026-07-19）**：
  S3 `DeleteObject` 可能已经提交 exact-version 删除，但客户端因超时、连接中断或响应
  丢失只收到错误。旧实现直接返回，不能在同一调用内区分“未提交”和“已提交但响应丢失”。
  这与参考 etcd `client/v3/retry.go` 的 `isSafeRetryMutableRPC` 原则一致：连接建立后的
  mutable write 不能盲重试，否则破坏 write-at-most-once；必须用可观察状态对账。

  现在删除错误后使用独立 5 秒预算，保留原 context values 但不继承取消信号，Head
  receipt 绑定的 bucket/key/version。只有明确 `NoSuchVersion`/NotFound 才确认已提交并
  发布确定性 deletion receipt；version 仍可读或 Head 失败时用 `errors.Join` 同时返回
  原删除错误和检查错误，不发布 receipt。成功响应后的验证也复用同一 exact-version
  helper。回归测试确定性覆盖提交后取消父 context 并丢失响应、未提交且仍可读、提交后
  无法检查三种结果。focused 连续 200 轮、focused race 50 轮、objectstore 全包连续
  50 轮、全包 race 20 轮、BackupDeletion 连续 10 轮 172.518 秒、production 全量
  91.684 秒和两侧 vet 均通过。代码提交
  `d5f30cc38df19fccdaa3e5a886dd9cf4b1c32654`。

  精确提交构建非 root TiKV 镜像 `kubebrain:a291-delete-write-reconcile`（image ID
  `sha256:79de7a9356e8a331dd40af4e86aef92fcaa184528c3d5f625234741970ed6767`）。
  真实 KubeBrain `/registry` 在 revision `467796068338761731` 导出 58 条记录、0 lease，
  artifact SHA-256 为
  `8f78a93cdb7d67a3c6a368408ac28b80ddb8bf7e8ff973b9c0a3e78f2feebd6d`。MinIO
  GOVERNANCE version `17c19d61-219b-4fdc-b6ff-535e41973c33` 保留到 `1784505138`；
  故障代理把三次已由 MinIO 成功处理的 DELETE 响应全部转换为 502，随后同一 CLI 调用
  Head 精确 version 得到 NotFound 并成功退出。deletion receipt SHA-256 为
  `2dcca9410e131da53950ebe8eaefc7dbb5e763b0a52a711c3316733020332c13`，测试 version
  已清理。archiver 与六类零副本 executor 模板更新到 A291；主 KubeBrain 3/3、
  PD/TiKV 3+3、A277 scheduler 2/2 当前均 Ready。

- **Backup/Audit A292 uncertain conditional Put reconciliation（2026-07-20）**：
  backup Upload 和 audit ArchiveAudit 过去只对明确 `PreconditionFailed` 做当前对象
  Head；普通超时、5xx 或响应损坏即使 `If-None-Match: *` Put 已提交也直接失败。参考
  etcd `client/v3/retry.go` 的 `isSafeRetryMutableRPC`，连接建立后的 mutable write 不能
  盲重试；条件写的可观察最终状态必须成为提交判断依据。

  两条路径现在对 generic Put 错误建立不继承父取消信号的独立 30 分钟证明预算。先 Head
  当前 version 并严格匹配 metadata/size，再 Head 精确 version 固化远端 LastModified，
  下载并解析完整 backup/audit artifact，最后复核 Object Lock mode 和 retain-until；
  全部通过才发布 receipt。30 分钟而非删除对账的 5 秒，是因为完整逻辑备份可能很大。
  明确 PreconditionFailed 的既有跨进程恢复仍使用调用 context。当前对象不存在、冲突或
  任一检查失败时用 `errors.Join` 保留原写错误且不发布 receipt。测试覆盖 Put 提交后
  取消父 context/丢失响应，以及未提交后的 NotFound；第一次只保护 Head 的实现被测试
  捕获为后续 exact-version 校验使用 canceled context，修复后整条证明链共享独立预算。

  focused 连续 200 轮、focused race 50 轮、objectstore 全包连续 50 轮、全包 race
  20 轮、backup/audit/archiver 选择性连续 5 轮 112.416 秒、production 非缓存全量
  91.730 秒和两侧 vet 均通过。代码提交
  `0fe9e88f752761f3ae63ff6c742f4c5bcd764b86`。精确提交构建非 root TiKV 镜像
  `kubebrain:a292-object-write-reconcile`（image ID
  `sha256:b562f91cf2ef7b65d7fddc2b3b3d71f62def35c7ed2eee77352efa45db420240`）。

  故障代理在 MinIO 成功提交后把两次 Put 响应改为非重试 418。真实 KubeBrain
  `/registry` revision `467796068338761731` 的 58 条记录、0 lease artifact
  SHA-256 为 `f34ebd103ba3822158da6d1d3dde4b6318fc05b8d282908e1c332ecf5cb24c33`；
  backup version `e97a68d1-376c-4727-a5eb-0a98f29af7ed` 在同一调用恢复，receipt
  SHA-256 为 `67d819e38aabcb444aa0f1b5034d66552a1608aa152775a0919e1976972be464`。
  654 字节 canonical audit artifact SHA-256 为
  `99c44665a22f5cdab86b768eae572fde3ec83711ee82bd194f0c449f6b67fbbc`；audit version
  `0736a581-ddd7-405b-899c-fdd66f496b5c` 同样恢复，receipt SHA-256 为
  `d082755208f2394924277e3f5001fd479b640e7582890ae351e229b4900429d9`。两个
  GOVERNANCE version 到期后均已按精确 version 清理。archiver 与六类零副本 executor
  模板更新到 A292；主 KubeBrain 3/3、PD/TiKV 3+3、A277 scheduler 2/2 当前均 Ready。

- **Compatibility A293 topology-explicit differential gate（2026-07-20）**：
  对照 `/root/etcd/server/etcdserver/api/v3rpc/member.go` 的 `MemberList` 代理语义和
  `/root/etcd/client/v3/cluster.go` 的官方客户端入口，完整差分套件在独立
  `a293-diff2` TiKV keyspace 上执行。此前 `TestMemberListFlagsDifferentialAgainstReferenceEtcd`
  把 KubeBrain 成员数硬编码为 3，导致一次性单成员验证拓扑产生假失败，也无法显式证明
  发布环境使用了预期成员数。现由 `KUBEBRAIN_EXPECTED_MEMBER_COUNT` 声明拓扑，默认仍为
  生产基线 3；空值、非数字和非正数均失败关闭。一次性隔离实例可显式设为 1，成员响应
  的 header、leader/local member 和成员字段完整性断言保持不变。修改前全量
  Differential 在 244.067 秒内完成，除硬编码成员数 `want=3, got=1` 外，已执行的
  KV/Txn/Watch/Lease/Compact/HashKV/MemberList/namespace/client recipe 场景均通过；
  修改后 MemberList 定向差分通过，兼容模块全包在真实 TiKV/PD 隔离端点上 82.831 秒
  通过，成员数解析 race 连续 20 轮通过。未配置端点时全包按默认 `127.0.0.1:3379`
  失败属于验证命令配置错误，不计入产品结果。

- **Auth A294 response header revision（2026-07-20）**：对照
  `/root/etcd/server/etcdserver/apply/backend.go` 的 `newHeader` 及全部 Auth applier
  响应，AuthStatus、Authenticate 和 User/Role CRUD 的成功 header 必须携带当前用户
  MVCC revision；Auth mutation 自身不消耗该 revision。KubeBrain 原 `authRPCHeader`
  只返回空 header，统一拦截器虽补齐 cluster/member/term，却刻意不覆盖方法专属
  revision，因此所有 Auth 成功响应错误地报告 revision 0。

  新增官方 gRPC 双端差分，以前置 Put 固定当前 revision，并验证 AuthStatus、RoleAdd、
  RoleGet header revision 均与 Put 一致，同时保留非零 cluster/member/term 断言。修复前
  参考 etcd 三项均为 true、A292 KubeBrain 三项均为 false；修复后 handler 从 backend
  当前 revision 构造 Auth header。focused 连续 100 轮、focused race 连续 20 轮、
  `pkg/server/etcd` 全包 31.594 秒、全包 race 260.744 秒、根模块全量和
  `go vet ./...` 通过。首次根模块全量与仍监听 `12379/12380` 的参考 etcd 冲突，
  endpoint 消费端测试连接到参考 3.8 而失败；停止参考进程后 endpoint 定向和根模块
  全量均通过，不计为产品失败。提交
  `ce53dd9991153a7c79e0f947ee003776d33b2cad` 构建镜像
  `kubebrain:a294-auth-header`（image ID
  `sha256:1d685a98d86fd3f91bbc317296e2f77985891f2505d0fdddc9ab1c29e79cb3a1`）。
  真实 3 PD + 3 TiKV 的独立 `a294-auth` keyspace 上，新 header 差分连续 10 轮通过；
  再换全新 `a294-auth-lifecycle` keyspace 和全新参考 etcd 数据目录，完整 Auth 生命周期
  首轮通过。该生命周期测试要求两个空实例，首轮后只关闭 Auth 而不删除用户，因此不能
  在同一实例用 `-count>1` 重复，后续 AlreadyExists 不计为产品失败。

- **Auth A295 linearized response revision（2026-07-20）**：继续对照
  `/root/etcd/server/etcdserver/v3_server.go`：AuthStatus 通过 raft request，
  Authenticate 先做 `LinearizableReadNotify`，其余 Auth mutation 也在 raft apply
  时用 `newHeader`。A294 虽补了非零 revision，却直接读取本副本
  `GetCurrentRevision` cache；新副本追赶或 follower 暂时落后时可能成功返回陈旧
  header，与 etcd 的线性化点不符。

  所有成功 Auth RPC 现在在构造 header 前执行 leader revision barrier；barrier 推进
  本地 cache 后才返回 revision，失败则使用标准可重试错误失败关闭。Auth metadata
  mutation 仍不消耗用户 MVCC revision。回归测试模拟 cache=100、leader=200，要求
  AuthStatus 只返回 200；barrier 返回 storage unavailable 时必须得到 gRPC
  `Unavailable` 且无响应。focused 连续 100 轮、focused race 连续 20 轮、
  `pkg/server/etcd` 全包 31.804 秒、全包 race 256.380 秒、根模块全量、
  peer/leader/revision 服务测试和 `go vet ./...` 均通过。

  代码提交 `cbd13a216d5a96feeaa60c57c587d6f472f4ab44` 构建镜像
  `kubebrain:a295-auth-revision-barrier`（image ID
  `sha256:d9800850a5ea6a273bdbc1733c296555f00755fd403d7875f94859d1545c393b`）。
  独立 `a295-auth` keyspace 上 Auth header 差分连续 10 轮、完整 Auth 生命周期通过。
  主三副本随后从 A285 无中断滚动到 A295，3/3 Ready、0 restart；leader 写入测试键
  revision `467798570765385731` 后，两个 follower 与 leader 的 AuthStatus header 均
  精确返回该 revision，测试键已在 revision `467798570765385732` 删除。

- **Maintenance A296 Hash protocol differential（2026-07-20）**：审计
  `/root/etcd/server/etcdserver/api/v3rpc/maintenance.go` 的 `Hash` 与认证 wrapper。
  参考 etcd 先要求 root 权限，再计算 member-local backend hash，并把被 hash 的
  revision 放入 response header。KubeBrain 保留相同权限顺序；由于用户 MVCC 数据由
  TiKV 共享、committed revision cache 属于副本本地状态，计算前 best-effort 刷新
  revision，但 leader 暂时不可用时仍保留 member-local 诊断能力。

  新增官方 Maintenance/KV gRPC 双端差分门禁，验证首次 Hash header 不落后于前置
  Put、无写时 hash 与 revision 稳定、更新后 revision 严格推进且 hash 改变。测试刻意
  不比较 hash 数值：参考 etcd hash 的是 bbolt backend，KubeBrain hash 的是 TiKV 中
  保留的逻辑 MVCC 状态，物理编码不同不应伪装成数值兼容。参考 etcd 与真实
  3 PD + 3 TiKV、3 副本 KubeBrain 连续 10 轮结果一致。同期复核 auth/barrier
  ordering：reference MemberList 的 linearizable barrier 同样位于 auth 检查之前，
  不存在可修复差异；typed nil transaction oneof 经 protobuf wire 解码为默认嵌套
  message，按空 key 拒绝，不构成远程 nil dereference。

- **Lease A297 KeepAlive/Revoke buffered convergence（2026-07-20）**：跟进
  upstream `/root/etcd` commit `a81b6d623` 明确的官方 client/v3 契约：`Revoke`
  返回后，`KeepAlive` channel 仍可能交付撤销前已经缓冲的正 TTL 响应。该数量取决于
  stream 调度，不能错误收紧为“Revoke 后绝对零响应”；但客户端仍必须在下一次续租
  发现 lease 不存在后关闭 channel，且已绑定 key 和 lease 均不可恢复。

  新增官方 client/v3 双端生命周期门禁：先等待首个有效 KeepAlive，再 Revoke；
  允许任意数量但要求 ID/TTL 合法的已缓冲响应，同时要求 5 秒内 channel 关闭、
  key 消失且 TimeToLive 精确返回 `-1`。全新参考 etcd 与真实 3 PD + 3 TiKV、
  3 副本 KubeBrain 连续 20 轮均通过，每轮约一个 keepalive 周期后收敛，总计
  142.140 秒；清理测试噪声后双端再连续 2 轮、真实双端 race 3 轮 22.475 秒通过，
  根模块全量和 vet 通过。该审计未发现服务实现差异，因而只增加永久回归门禁，
  不制造无依据的运行时修改。

- **Lease A298 uncertain checkpoint write reconciliation（2026-07-20）**：继续对照
  `/root/etcd/server/lease/lessor.go` 的 periodic checkpoint 与 Renew clear ordering。
  KubeBrain 已用 `leaseCheckpointMu` 排序 checkpoint/renew，但 TiKV `InternalPut`
  可能已经提交 remaining TTL record、客户端却只收到取消或传输错误。旧实现此时不更新
  内存 `remainingTTL`；若 KeepAlive 在一秒 checkpoint retry 前续租，它会把内存 deadline
  恢复到完整 TTL，却因内存仍为 0 而不清除实际 durable checkpoint。下一次 leader
  reload 会按旧 remaining TTL 恢复，导致刚续租的 lease 提前过期。

  lease canonical metadata 写现在对任意失败使用不继承父取消信号的独立 5 秒预算，
  线性化读取同一 internal key；只有字节完全等于本次 `{id,ttl,remainingTTL}` record
  才确认已提交。检查失败或当前值不同使用 `errors.Join` 保留原写错误并 fail closed。
  该边界同时保护 Grant metadata、周期 checkpoint 和 renew checkpoint clear。确定性
  回归让底层先提交 240 秒 checkpoint、取消父 context 再丢失响应；修复前随后 KeepAlive
  返回 600 秒但 durable record 仍为 240，修复后精确清零。另固定未提交写必须报告状态
  漂移。三组 focused 连续 100 轮、focused race 30 轮、Lease 全组连续 20 轮
  88.391 秒、server 全包 31.508 秒、根模块全量和 vet 均通过。代码提交
  `d69fe1384f07579b00db8cae1af7c0bc067509f3`。

  从该提交的 `git archive` 构建非 root 镜像
  `kubebrain:a298-lease-metadata-reconcile`（image ID
  `sha256:bfe586feb2f03cd8521423b460a00b4dcb346e8a2565a3504b4604a3aef5b2b4`），
  避免把工作区未提交文件混入 SHA 证据。三副本 StatefulSet 无中断滚动后，三个 direct
  replica 的 Grant/TTL/attached-key/Revoke 连续 20 轮通过，Watch+Lease 与 atomic
  attached-key revoke 各连续 10 轮通过。运行验证同时发现六个 failover 门禁仍硬编码
  等待旧 `deployment/kubebrain`；提交 `4b88d9d` 改为默认等待生产
  `statefulset/kubebrain`，并允许一次性拓扑显式覆盖。最终连接固定在 follower
  `kubebrain-0`，删除 leader `kubebrain-1` 后 authoritative Lease read 门禁
  7.13 秒通过，无陈旧成功响应；StatefulSet 恢复 3/3 Ready、0 restart，PD/TiKV
  3+3 Running。

- **Lease A299 uncertain revoke reconciliation（2026-07-20）**：继续审计 lease
  metadata 删除与 attachment 更新的 TiKV 模糊提交边界。普通 Put/Delete/Txn 已把用户
  KV 与 attachment 放在同一 `TxnApply`，并有 revision-aware index reconciliation；
  但 lease revoke 的原子事务若已经删除全部 key、attachment 和 metadata，响应却以
  `ErrUncertainResult` 丢失，旧实现仍保留内存 lease。后续 KeepAlive 可以把已删除的
  metadata 重新写回，形成无 attached key 的 lease 复活，并让当前 leader 与 durable
  state 持续分歧。

  revoke 现在只对 `ErrUncertainResult` 使用不继承调用方取消信号的独立 5 秒预算，
  线性化读取该 lease 的 canonical metadata key。metadata 缺失可证明同一 TiKV 原子
  transaction 中的 key、attachment 与 metadata 删除全部提交，此时 revoke 按成功
  收敛并清除内存 lease；metadata 仍存在或检查失败则保留原 uncertain error 和内存状态，
  fail closed。确定性测试分别注入 commit-then-error 与未提交 uncertain result，固定
  前者必须返回成功、key/attachment/metadata/TTL 全部消失，后者必须保留 key、metadata
  与 attached-key TTL。focused 连续 100 轮、race 连续 20 轮、server 全包
  31.703 秒、根模块全量（production 91.161 秒）和 `go vet ./...` 均通过。代码提交
  `f5b6b8448108a171adf7263717f1515adaaa644c`。

  从该提交的 `git archive` 构建非 root 镜像
  `kubebrain:a299-uncertain-revoke-reconcile`（image ID
  `sha256:11465cfc4bed6aeb90404ad1bc6033f3f917276b1da6057e3014b451744e667a`，
  embedded version `3.7.0-dbaas.a299`、完整 SHA、用户 `65532:65532`）。三副本
  StatefulSet 无中断滚动后，direct replica Lease 生命周期连续 20 轮通过；两个独立
  KubeBrain replica endpoint 的 KeepAlive/Revoke 收敛连续 10 轮、71.754 秒通过。
  固定 follower `kubebrain-0` 并删除 leader `kubebrain-2` 的 authoritative Lease
  read failover 门禁 7.49 秒通过，选举期间只有预期 deadline failure、无陈旧成功响应。
  最终 KubeBrain 3/3 Ready、0 restart，独立 PD/TiKV 3+3 Running。

- **Lease A300 priority revoke admission under overload（2026-07-20）**：跟进
  upstream `/root/etcd` commit `59ce0ce31` 的 `PriorityRequest` 过载语义。etcd 在
  Raft committed/applied backlog 超过普通阈值后仍为 LeaseRevoke 保留一段有界空间，
  避免普通写流量让租约无法及时释放。KubeBrain 没有本地 Raft apply backlog，对应的
  生产过载边界是 public client 的 process-wide inflight 和 QPS token bucket；旧实现
  在两者耗尽时都把 LeaseRevoke 与普通 Range/Put 一样直接
  `ResourceExhausted`，attached key 只能等待自然过期。

  启用任一 admission limit 时，现从该 limit 派生只允许
  `/etcdserverpb.Lease/LeaseRevoke` 使用的 10% reserve（向上取整、至少 1）：
  普通请求在原 limit 处继续 fail closed，不能占用 reserve；revoke 先使用共享预算，
  仅在共享预算耗尽后使用独立 inflight/token reserve，reserve 本身耗尽后同样拒绝，
  不形成无限旁路。新增
  `grpc.server.admission.priority_admitted` 与
  `grpc.server.rate_limit.priority_admitted` 计数，使容量耗尽但安全回收仍成功的状态
  可观测。生产默认 1024 inflight 对应 103 个 revoke reserve；2000 QPS/4000 burst
  分别对应 200/400 reserve。

  容量、隔离和拒绝边界 focused 连续 100 轮，完整 admission 组连续 30 轮、race
  连续 20 轮，server 全包 31.601 秒；根模块全量（production 90.122 秒、
  backend 41.467 秒、server/etcd 31.818 秒）和 `go vet ./...` 均通过。运行时代码提交
  `09e1b67514bdf45d89301a9fa2378aa97fb43589`，永久黑盒门禁提交
  `28bf3bc224e05006081ae5c181f0a9bcf9fffa58`。

  从 runtime 提交的 `git archive` 构建非 root 镜像
  `kubebrain:a300-priority-revoke-admission`（image ID
  `sha256:d1104191fa43f2636f220177c3cd87b58a2ad4850b568bfa06c78eefa1ad3d85`，
  embedded version `3.7.0-dbaas.a300`、完整 SHA、用户 `65532:65532`）。三副本
  StatefulSet 无中断滚动后，将专用验证实例的
  `--max-requests-inflight` 从 1024 暂降到 1；固定 `kubebrain-0` endpoint，以已创建
  Watch 占满唯一普通槽位，raw Range 每轮均返回 `ResourceExhausted`，同连接
  LeaseRevoke 仍通过 reserve 成功并删除 attached key，连续 20 轮 2.807 秒通过。
  参数随后恢复 1024 并完成第二次滚动；标准 Kubernetes Watch+Lease、atomic revoke
  和 signed lease ID 生命周期连续 20 轮 9.198 秒通过。最终 KubeBrain 3/3 Ready、
  0 restart，PD/TiKV 3+3 Running，未遗留 port-forward。

- **Lease A301 explicit-ID generation linearizability（2026-07-20）**：扩展 A40
  Porcupine lease lifecycle，新增显式 ID 的 Grant、lease-bound Put、Get、
  TimeToLive、Revoke 和同 ID regrant 代际模型。确定性模型反例要求成功 Revoke
  原子删除旧 attached key，成功 regrant 必须从无 key 的新代际开始，并拒绝已完成的
  旧 revoke 删除新代际 key；Grant 的 `lease already exists`、Put/Revoke 的
  `lease not found` 作为确定性结果，只有 transport/timeout 类错误按可能提交或未提交
  分叉。client/v3 高层 Grant 不接受显式 ID，门禁通过同一官方 client connection 上的
  generated `LeaseGrantRequest{ID: ...}` 覆盖真实 wire API。

  模型正反例连续 100 轮、race 连续 50 轮通过；A300 精确运行镜像上的三副本
  KubeBrain + 独立三 PD/三 TiKV，以 NodePort 直接执行 20 轮无故障历史，共 1000
  个并发操作全部为 `Ok`。随后删除当前 leader `kubebrain-1`，150 操作历史捕获 26
  个不确定 RPC（包括 proxy not-ready 和 revision deadline），并观测到恢复后的确定性
  LeaseExist/LeaseNotFound，完整历史仍为 `Ok`。副本恢复后，旧 lifecycle 与新
  generation history 组合连续 5 轮 7.735 秒通过。最终 KubeBrain 3/3 Ready、0
  container restart，PD/TiKV 3+3 Ready，未遗留 port-forward。该门禁未复现运行时
  兼容性差距，因此 A301 只增加永久正确性证据，不重建与 A300 runtime 相同的镜像。

- **Lease A302 watch-backed natural-expiry linearizability（2026-07-20）**：继续扩展
  A301 代际模型，增加 KeepAlive 与 synthetic natural-expire 操作。对照 upstream
  `/root/etcd/server/lease/lessor.go` 的 expired lease revoke/renew 排序，以及
  commit `943f4d296` 的 renew 后置存在性复检：自然过期不能只由 wall-clock sleep
  推断完成。新门禁以 lease DELETE watch event 为权威完成证据，把 expire 操作的
  call 定位在 Grant/KeepAlive 后最早可能 deadline，把 return 定位在客户端实际收到
  DELETE；窗口内持续并发 Get 与 TimeToLive，Porcupine 因而可以合法排列尚未完成的
  expiry，同时拒绝“expiry 已完成但 lease 或 attached key 仍存活”。纯模型反例明确
  钉住 lease/key 必须在同一个状态转换中死亡。

  `TestClientV3LeaseNaturalExpiryHistoryIsLinearizable` 连续使用同一显式 ID 执行三轮
  Grant、leased Put、自然过期和 regrant，其中一轮在旧 deadline 前 KeepAlive；
  Watch 在 leader/follower transport 关闭时按最后已见 revision 无损续订，不能因
  failover 丢失 DELETE 后误报成功。A300 精确运行镜像上的三副本 KubeBrain + 独立
  三 PD/三 TiKV 无故障连续 5 轮 36.072 秒通过。删除权威 `etcdctl endpoint status`
  确认的 leader 后，恢复 Watch 捕获自然 DELETE，完整历史 7.906 秒返回 `Ok`。
  恢复后与 TTL=0、expired KeepAlive deletion ordering、旧 deadline callback fencing
  三项既有门禁组合连续 3 轮 46.114 秒通过；模型正反例连续 100 轮、race 连续 50
  轮通过。最终 KubeBrain 3/3 Ready、0 container restart，PD/TiKV 3+3 Ready，
  未遗留 port-forward。未复现运行时兼容性差距，因此不重建相同 runtime 镜像。

- **Observability A303 metrics-before-handler interceptors（2026-07-20）**：跟进
  upstream `/root/etcd` commit `0c68e485a`。etcd 将 server metrics unary/stream
  interceptor 移到 require-leader 等 handler-specific interceptor 之前，否则被
  前置拒绝的请求不会进入 `grpc_server_handled_total`，过载、无 leader 和 client
  cancellation 的错误率会被系统性低估。KubeBrain 的 endpoint 已先追加 metrics
  `UnaryInterceptor`/`StreamInterceptor`，再追加 client admission 或 peer
  require-leader 的 `Chain*Interceptor`；grpc-go 会把单 interceptor 放在 chain
  外层，现有顺序与 upstream 一致，但此前没有永久测试保护，重排 options 即可静默
  破坏生产告警。

  将 client/peer gRPC option 装配提炼为显式方法并标注该顺序契约，新增
  `TestGRPCMetricsObserveCallsRejectedBeforeHandlers`：client 与 peer 两条 listener
  路径分别以 unary Health Check 和 stream Health Watch 触发 handler 前
  `ResourceExhausted`，外层 metrics 必须同时观测最终 code；若顺序回归，测试在 1 秒
  内明确失败。focused 连续 100 轮、race 连续 50 轮通过，endpoint 与 Prometheus
  metrics 全包 20.034/0.021 秒通过，相关 `go vet` 通过。真实 A300 三副本 info
  endpoint 同时确认已有
  `grpc_server_handled_total{grpc_code="Canceled",grpc_method="Watch",...}` 计数，
  证明运行组合中的取消 stream 未绕过 metrics。最终 KubeBrain、PD、TiKV 均 3/3
  Ready，未遗留 port-forward；本轮只显式化并锁定现有正确行为，不需重建 runtime。

- **Auth A304 nested Txn/PrevKV/leased Put differential（2026-07-20）**：对照
  upstream `/root/etcd` commits `204097b19`（递归检查嵌套 Txn 两个分支）与
  `70a2b4871`（Txn PrevKV 及带 lease Put 授权）。代码审计确认 KubeBrain 已递归
  授权嵌套 compare/success/failure 操作；Put PrevKV 同时要求 key 的 READ/WRITE，
  带 lease Put 还会在 leader admission 后持锁复检目标 lease 及其全部既有 attached
  key 的 WRITE 权限。因此本轮没有修改运行时，而是把这些容易在事务重构中退化的规则
  加入永久黑盒差分矩阵。

  新矩阵覆盖两层嵌套的拒绝 Range、未选中 else 分支中的拒绝 Put、带 PrevKV 的拒绝
  Delete、绑定受保护 lease 的 Put，以及 write-only 用户执行 PrevKV Put；并由 root
  复读确认被拒绝的 Put/Delete/PrevKV 均未改变原值。fresh upstream etcd 与认证状态
  为空的隔离 restore KubeBrain 实例逐字段差分通过（4.397 秒）。主三副本实例已有其他
  认证测试遗留的受保护对象，本轮没有为追求空环境而破坏该状态，只执行健康门禁；临时
  NodePort Service 已删除，全程未使用 port-forward。

  相关确定性单测连续 50 轮通过（87.379 秒），关键授权门禁 race 连续 20 轮通过
  （446.663 秒），完整 `TestAuth` 测试组通过（12.865 秒），compat 测试在无外部
  endpoint 时连续 20 轮完成编译/skip 门禁，`go vet ./pkg/server/etcd` 通过。最终
  KubeBrain 3/3 Ready 且 0 restart，PD/TiKV 3+3 Ready；未发现新的运行时兼容性差距，
  不重建与 A300 相同的 runtime 镜像。

- **Watch A305 canceled-create response header（2026-07-20）**：对照 upstream
  `/root/etcd` commit `f5912263c`。该修复为 grpcproxy 拒绝无效 Watch range 时返回的
  `Created && Canceled` 响应补齐非 nil `ResponseHeader`；缺失 header 会破坏依赖
  create revision 建立恢复下界的官方 client/v3 消费端。审计确认 KubeBrain 的负
  start revision、鉴权拒绝、重复 Watch ID、空/逆序 range 和 Watch 配额五类本地拒绝
  已各自携带当前 revision header，但此前由五处手工字面量维持，新增路径容易静默漏掉。

  五类路径现统一使用 `canceledWatchCreateResponse`，构造器固定 `WatchId=-1`、
  `Created=true`、`Canceled=true` 和非 nil revision header；表驱动测试覆盖全部既有
  reason 以及空 reason，源码审计只允许该构造器生成 canceled-create 响应。构造及
  Watch ID/range 单测连续 100 轮通过（1.770 秒），focused race 连续 50 轮通过
  （4.418 秒），完整 Watch 服务测试通过（2.525 秒），`go vet ./pkg/server/etcd`
  通过。

  fresh upstream etcd 与 A300 三副本 TiKV-backed KubeBrain 的 Watch ID/range wire
  差分连续 20 轮通过（0.816 秒）：重复 ID、空 range、逆序 range 均返回正 revision
  header，错误后同一 multiplexed stream 仍可继续创建和取消 Watch。参考进程及数据
  目录已清理，未使用 port-forward；最终 KubeBrain 3/3 Ready 且 0 restart，
  PD/TiKV 3+3 Ready。本轮把已正确的运行语义收敛为不可绕过的构造契约，线上 A300
  镜像的 wire 证据已一致，因此不重建语义相同的 runtime 镜像。

- **Range A306 limited KeysOnly with exact total count（2026-07-20）**：对照
  upstream `/root/etcd` commit `dd57ad39f`。该修复使内存 treeIndex 在
  `FastKeysOnly + Limit` 下只收集页面内 key，同时可继续遍历索引得到精确 total
  count，避免为有限响应构造整个范围的 KeyValue。KubeBrain 没有 etcd 的本地 MVCC
  treeIndex，但现有 TiKV 路径已采用等价且更适合独立存储的数据流：对象扫描只请求
  `Limit+1` 判断 `More`，精确 Count 由 revision-aware count index 提供，随后只转换
  页面内 KeyValue 并清空 Value。

  新增存储读取上界门禁，创建 100 个带较大 value 的 key 后执行
  `KeysOnly + Limit=3`：响应必须只含 3 个空 value key、`Count=100`、`More=true`，
  底层最多读取 10 行。实测为 9 行，包括 4 个页面候选和用户边界低字节扩展的固定安全
  探测；该上界与范围总大小无关，防止 count index 或页面 limit 回归后退化为 O(N)
  value 扫描。确定性门禁连续 50 轮通过（1.408 秒），race 连续 20 轮通过
  （3.287 秒），完整 Range 服务测试通过（2.613 秒）。

  官方 client/v3 双端差分固定三组显式预期：当前 revision 下删除后的前三个 key、
  `Count=11/More=true`；历史 revision 下原前三个 key、`Count=8/More=true`；Limit
  大于总数时返回全部 11 个 key 且 `More=false`，所有 Value 均为空。fresh upstream
  etcd 与 A300 三副本 TiKV-backed KubeBrain 连续 20 轮通过（5.770 秒）。根模块及
  compat 模块相关 `go vet` 通过，参考进程和数据目录已清理，未使用 port-forward；
  最终 KubeBrain 3/3 Ready 且 0 restart，PD/TiKV 3+3 Ready。未发现运行时语义或
  读放大差距，因此本轮只增加永久生产性能证据，不重建相同 runtime 镜像。

- **Range A307 revision-visible keys across tombstones（2026-07-20）**：对照
  upstream `/root/etcd` commit `8ce417fa0`。该改动将 treeIndex `Revisions` 的
  Limit 判断移到每个 key 的 revision 可见性检查之前，并与 `Range` 统一：Limit
  只按目标 revision 下存活的 user key 消耗，tombstone、尚未创建或已删除的 key
  不能占据页面槽位；需要 total count 时则继续遍历其余可见 key。

  KubeBrain 的 TiKV scanner 不使用 treeIndex Revisions，但已有等价数据流：同一 user
  key 的物理历史版本先折叠为目标 revision 下的最新状态，仅非 tombstone 状态才 append
  到 limited receiver；receiver 满后停止，精确 Count 独立由 revision-aware count
  index 计算。新增服务层三阶段门禁：四 key 删除前的历史 revision 以 Limit=2 返回
  `a,b / Count=4`；删除 `b,d` 后的 revision 以 Limit=1 返回
  `a / Count=2`；更新 `c`、重建 `b` 并新增 `e` 后，当前 revision 再次返回
  `a,b / Count=4`。三阶段均要求 `More=true` 且 KeysOnly value 为空。

  服务门禁连续 50 轮通过（1.124 秒），focused race 连续 30 轮通过（3.233 秒），
  完整 Range 服务测试通过（2.649 秒）。fresh upstream etcd 与 A300 三副本
  TiKV-backed KubeBrain 对上述三组显式预期连续差分 20 轮通过（4.401 秒）；根模块
  与 compat 模块相关 `go vet` 均通过。参考进程和数据目录已清理，未使用
  port-forward；最终 KubeBrain 3/3 Ready 且 0 restart，PD/TiKV 3+3 Ready。
  未发现运行时差距，因此本轮增加永久历史可见性证据，不重建相同 runtime 镜像。

- **Lease A308 follower KeepAlive cancellation status（2026-07-20）**：对照
  upstream `/root/etcd` commit `b54c88406`。旧 etcd Lease server 在 stream context
  被取消时把所有 `context.Canceled` 重写为 `ErrGRPCNoLeader/Unavailable`，因此客户端
  主动取消一个经 follower 转发的 KeepAlive，服务端 metrics 却记录
  `Unavailable`，污染可用性 SLO 和无 leader 告警。upstream 现保留 context 原始错误，
  只有 require-leader monitor 自身负责产生 NoLeader。

  KubeBrain 的 follower 路径把原 stream context 直接传入 leader unary 转发，返回值
  原样向上交付，不存在该错误重写。新增阻塞转发单测：确认请求已进入 peer
  `LeaseKeepAlive` 后取消客户端 context，leader 调用必须立即看到同一 cancellation，
  follower handler 返回 `context.Canceled`，且不能发送伪响应。确定性门禁连续 100
  轮通过（1.575 秒），focused race 连续 50 轮通过（3.729 秒），完整 Lease 服务测试
  通过（9.342 秒）。

  新增可重复 live compat 门禁，以显式 follower endpoint 和该 Pod metrics URL 批量
  取消 200 条 KeepAlive stream，并在前后解析
  `grpc_server_handled_total`。A300 三副本中的 follower `kubebrain-0` 实测 0.148 秒
  通过：94 条实际进入服务端 handler，最终
  `LeaseKeepAlive/Canceled=94`，`Unavailable=0`；未进入 handler 的连接由客户端在
  admission 前取消，不应计入服务端。根模块与 compat 模块相关 `go vet` 均通过，
  临时 follower client/metrics NodePort Service 已删除，未使用 port-forward。
  最终 KubeBrain 3/3 Ready 且 0 restart，PD/TiKV 3+3 Ready；现有 runtime 已正确，
  本轮不重建语义相同的镜像。

- **RangeStream A309 successful termination with EOF（2026-07-20）**：对照
  upstream `/root/etcd` commit `967feb0d6`。etcd grpcproxy 的进程内
  `chanStream` 原先在 server-streaming handler 成功返回 nil 后只关闭 context，
  没有向 client side 投递 terminal status；`RecvMsg` 因而可能挂起或返回 context
  错误，而 gRPC ClientStream 契约要求正常完成严格返回 `io.EOF`。upstream 现把 nil
  handler 结果显式转换为 EOF。

  KubeBrain 不使用该 chan adapter，RangeStream handler 成功返回 nil 后由真实 grpc-go
  transport 产生 EOF；follower 也先执行 read barrier 再从本地 durable TiKV snapshot
  流式读取，不经过另一套会吞掉 terminal status 的内存代理。已有 bufconn 测试只隐式
  覆盖非空 recursive range，本轮将 common-shape 双端结果增加显式
  `EndedWithEOF`，并逐项 fail closed 断言 point hit、point miss、相等空区间、逆序空
  区间、Limit、历史 revision 和 from-key 七种成功形状都没有 terminal gRPC code。

  fresh upstream etcd 与通过专属 NodePort 明确直连的 follower `kubebrain-0` 连续
  20 轮双端差分通过（5.132 秒），七种形状全部 EOF 且内容、Count、More、header 与
  unary Range 一致。bufconn 真实 gRPC EOF/terminal metadata 门禁 race 连续 50 轮
  通过（5.051 秒），完整 RangeStream 服务测试通过（0.667 秒），compat race
  编译/skip 连续 50 轮通过（1.092 秒），根模块与 compat 模块相关 `go vet` 通过。
  参考进程、数据目录和临时 follower Service 均已清理，未使用 port-forward；最终
  KubeBrain 3/3 Ready 且 0 restart，PD/TiKV 3+3 Ready。现有 runtime 已正确，本轮
  不重建语义相同的镜像。

- **Read A310 non-reused freshness fence after cancellation（2026-07-20）**：
  对照 upstream `/root/etcd` commit `e2f4f485e`。etcd 的 ReadIndex loop 原先在首次
  发送及 first-commit/timeout 重试时复用同一个 request ID，延迟的旧 ReadState
  可能被误认作当前响应；修复后每次发送使用唯一 ID，并接受本轮已发送 ID 集中的响应。
  该规则与 A249 的 leader-change response fencing 互补：即使 leader identity 未变，
  后到 reader 也不能消费在自己 call 之前已发起的 freshness fence。

  KubeBrain 不使用 Raft ReadIndex，而是从 leader `/status` 获取 TiKV committed
  revision；现有 double-buffer fetch 已保证中途到达的 reader 进入 `next`，下一次
  HTTP fetch 只会在当前 fetch 完成后启动，因此每个 reader 得到的 fence 都在其 call
  之后发起。新增取消反例：reader A 启动并阻塞 F1 后取消，reader B 才到达；尽管共享
  F1 继续收尾，B 必须排入 F2，只能得到 revision 200，不能泄漏 F1 的 revision 100。
  该测试与既有 mid-flight reader 门禁组合连续 100 轮通过（1.653 秒），race 连续
  50 轮通过（2.173 秒），revision 全包通过（5.079 秒）。

  A300 三副本上通过专属 endpoint 确认当前 leader 为 `kubebrain-2` 后删除该 Pod，
  5 client 的 150 次并发 Range/Put/CAS 捕获 4 个不确定 RPC，包括旧 leader DNS
  消失、proxy not-ready 和 transport close；Porcupine register 完整历史仍为 `Ok`
  （6.361 秒）。替换 Pod Ready 后无故障历史连续 5 轮通过（2.453 秒）。根 revision
  包与 compat 模块相关 `go vet` 通过，三个临时直连 Service 已删除，未使用
  port-forward；最终 KubeBrain 3/3 Ready 且 0 restart，PD/TiKV 3+3 Ready。现有
  runtime 已正确，本轮不重建语义相同的镜像。

- **Observability A311 recovered etcd version metrics（2026-07-20）**：对照
  upstream `/root/etcd` commit `f55d8a061`。该修复在 server `Recover()` 路径恢复
  `etcd_cluster_version` gauge，避免进程重启后只有内存中的集群版本恢复、对应
  Prometheus 指标却持续缺失。A300 三副本运行时实测同时缺少
  `etcd_server_version` 和 `etcd_cluster_version`，因此这不是仅需测试覆盖的等价
  实现，而是实际可观测性差距。

  KubeBrain 现固定公开 etcd 兼容版本 `3.7.0` 和集群主次版本 `3.7`，并在
  `New` 完成 lease 恢复后发出 `etcd.server.version` 与
  `etcd.cluster.version` gauge；Prometheus adapter 最终分别生成
  `etcd_server_version{cluster="default",server_version="3.7.0"}` 和
  `etcd_cluster_version{cluster="default",cluster_version="3.7"}`。精确 mock
  门禁固定 metric 名、label、值及完整版本与主次版本关系：确定性连续 100 轮通过
  （0.075 秒），race 连续 100 轮通过（1.375 秒），Maintenance status/version
  组合连续 20 轮通过（0.800 秒），`go vet ./pkg/server/etcd` 通过。

  发布镜像 `kubebrain:a311-version-metrics` 由完整代码提交
  `6d1717727e968438fbb26f7214d7b8943bd112b5` 构建，本地镜像 digest 为
  `sha256:e397b0ca246467ea1af2952c765677b78bed721c9a3b38952815765c6dffc739`；
  首次故意误用短 SHA 的构建被既有 release metadata gate 正确拒绝。三副本
  StatefulSet 有序滚动后，三个 Pod 均直接抓取到上述两项精确指标，`/version` 返回
  server/cluster `3.7.0/3.7.0`，etcdctl Status 返回 version/storage version
  `3.7.0/3.7.0`。确认 `kubebrain-0` 为 follower 后删除并重新创建该 Pod，恢复后的
  两项指标、member ID 和 follower 身份均保持正确，三个 Pod 最终 Ready 且
  restartCount=0。生产 release gate 以精确镜像名验证 KubeBrain 3/3、PD/TiKV 3+3
  和 endpoint proposal health 全部通过，证明版本指标在正常启动和持久状态恢复后
  都不会丢失。

- **Lease A312 repeated-leader renewal soak（2026-07-20）**：对照 upstream
  `/root/etcd/client/v3/lease.go` 的 `KeepAlive`、`resetRecv`、
  `sendKeepAliveLoop` 和 `deadlineLoop`。clientv3 在 keepalive stream 断开后必须
  重建 stream、重新发送所有到期 lease ID，并在原 TTL deadline 前收到正 TTL 响应；
  否则即使一次 `KeepAliveOnce` 或单 lease failover 通过，批量长期连接仍可能静默关闭
  channel 并让 attached key 过期。现有 A240 只覆盖 256 个短
  Grant/KeepAliveOnce/TTL/Revoke 周期，已有 failover 门禁也只绑定单 session 或单
  lease，不能证明多客户端持续 stream 跨连续 leader replacement。

  新增显式 opt-in 的破坏性门禁：8 个独立 clientv3 client 各维持 8 个 TTL=30s lease
  及 attached key，共 64 条持续 KeepAlive；外部命令每轮动态发现并删除当前 leader，
  连续执行 3 轮。每轮替换完成后，测试为每条 lease 记录响应水位，要求全部 64 条都
  收到严格更新的正 TTL 响应，再逐 key 验证仍存在并绑定原 lease；任一 channel 提前
  关闭、错误响应、响应停滞、key 丢失或 lease 改绑均立即失败。结束时主动停止 stream、
  revoke 全部 lease，并确认隔离前缀为空。未显式设置 failover command 时测试默认
  skip，避免在共享或生产实例误执行 Pod 删除。

  compat 确定性 skip/compile 连续 100 轮通过（0.028 秒），race 连续 50 轮通过
  （1.127 秒），compat `go vet` 通过。A311 三副本 TiKV-backed KubeBrain 上普通与
  race 两轮真实门禁分别在 64.69/64.41 秒通过，共完成 6 次当前 leader replacement；
  首轮后三个 Pod UID 均变化，证明三轮不是重复删除旧对象或空操作。每轮全部 64 条
  stream 恢复并保持 key/lease 绑定，最终测试前缀无残留，KubeBrain 3/3 Ready 且
  restartCount=0，PD/TiKV 3+3 Ready，endpoint proposal health 通过。本轮未发现
  runtime 差距，只增加永久故障门禁，不重建 A311 镜像；小时级、跨可用区和网络分区
  soak 仍属于 P2 未完成项。

- **Watch A313 quota release across transport/Pod replacement（2026-07-20）**：
  对照 upstream `/root/etcd/server/etcdserver/api/v3rpc/watch.go` 的单 gRPC stream
  多 logical Watch 控制响应语义。A80 已以 `max-watches=1` 证明显式 cancel 后槽位
  可复用，单测也固定 `watcher.Close` 同步释放；但真实 transport 强制断开和承载进程
  UID replacement 尚无门禁。该缺口不能由 A232 的 RPC in-flight replacement 代替：
  一个 Watch RPC slot 内可 multiplex 多个逻辑 Watch，两套计数及释放生命周期不同。

  新增显式 opt-in 的 protobuf client 故障门禁，要求专属 Pod endpoint 和临时
  `max-watches=2`。测试先在一个 stream 创建两个 Watch 并确认第三个收到
  `Created=true,Canceled=true,WatchId=-1,CancelReason="etcdserver: too many
  requests"`，随后直接关闭底层 gRPC connection、不发送 cancel；新 connection 必须
  立即再次接纳两个 Watch。第二阶段保持两个槽位占用并删除 victim Pod，要求旧 stream
  在 30 秒内终止、同名 Pod 以新 UID Ready、专属 endpoint 确认可路由到 replacement，
  再次接纳两个 Watch 且拒绝第三个。helper 按 multiplex 契约跳过可穿插在 create ack
  前的非 Created progress 响应，避免把合法 progress 错判为配额响应。

  默认 skip/compile 连续 100 轮通过（0.038 秒），compat `go vet` 通过。首轮真实门禁
  9.91 秒通过；重复运行最初暴露 Pod Ready 早于 EndpointSlice/NodePort 更新的短暂
  `connection refused`，门禁现把固定 endpoint TCP 可达纳入恢复条件。修正后 A311
  三副本 TiKV-backed KubeBrain 普通 5 轮在 50.60 秒通过，race 3 轮在 30.33 秒通过；
  加首轮共完成 9 次成功的 victim UID replacement。每轮均固定断连后的两个槽位完整
  释放、旧进程 stream 关闭和新进程从零接纳；生产 `max-watches=10000` 已恢复，临时
  Service 已删除。最终 KubeBrain 3/3 Ready 且 restartCount=0，PD/TiKV 3+3 Ready，
  exact A311 image、endpoint proposal health 和 production release gate 通过。本轮
  未发现 runtime 差距，只补齐永久配额故障证据，不重建相同镜像。

- **Metering A314 immutable hourly sample archive（2026-07-20）**：关闭计量原始
  证据的不可变留存缺口，不把该能力误作最终计费。新增
  `kubebrain.metering-sample.v1` canonical artifact：每次处理由当前时间减去 10 分钟
  finalization delay 后截断到整小时，固定 slot start/end、artifact ID 和对象键；先
  要求 `kubebrain_dbaas:metering_data_complete` 唯一等于 1，再逐条查询 8 个既有
  recording rule。每条序列必须唯一、精确带目标 `dbaas_instance`、值非负且有限、
  timestamp 不晚于查询时间且不超过 5 分钟陈旧，否则整个槽位 fail closed。

  对象存储模块新增通用 typed immutable blob archive。上传限制为非空且不超过 16 MiB，
  使用 SHA-256 checksum、`If-None-Match: *`、Object Lock mode/retain-until 和业务
  元数据；只接受对象存储返回的 exact version ID。上传后按 exact version 重新下载并
  逐字节校验，再读取远端 retention；只有全部匹配才原子发布
  `kubebrain.object-immutable-blob.receipt.v1`。若 Put 返回不确定错误或条件冲突，只在
  已存在对象的 format、artifact ID、instance、store、digest、大小、保留期和内容全部
  匹配时恢复同一收据，冲突对象绝不覆盖。生产 CronJob 每小时 UTC 第 17 分钟运行，
  `Forbid` 并发、900 秒启动/运行 deadline，默认 COMPLIANCE 保留 7 年，使用独立
  Object Store Secret、无 ServiceAccount token、非 root 和只读根文件系统。

  计量包普通测试通过（0.058 秒），确定性连续 100 轮通过（2.435 秒），race 连续
  20 轮通过（2.548 秒）；对象存储模块普通/race/vet、生产 manifest 测试与
  `kubectl apply --dry-run=client`、根模块全量测试和 `go vet ./...` 均通过。真实
  MinIO bucket 启用 versioning/Object Lock 后，同一小时槽位连续归档两次得到相同
  receipt，独立检查只存在一个 938-byte exact version，COMPLIANCE retention、
  SHA-256 checksum/metadata 和业务 metadata 全部匹配。该 integration test 保留为
  显式 opt-in 门禁，不在缺少专用 Object Lock store 时静默模拟成功。

  发布镜像 `kubebrain:a314-metering-archive` 由完整代码提交
  `4cacd189afd87c74842223b741c0da93c0d07a1f` 构建，本地镜像 digest 为
  `sha256:a8a1796e57de6b9757c76ef3f48218d45d07df7290f0381becadc90ed163a4d5`；
  OCI revision、version 和两个新增二进制均已在容器内核对。A311 三副本
  StatefulSet 有序滚动到 A314 后 3/3 Ready、restartCount=0，PD/TiKV 3+3 Ready，
  etcdctl Status 仍返回 3.7.0，endpoint proposal health 和 production release gate
  全部通过。不可变小时采样留存至此完成；跨周期积分、价格版本、对象存储成本和审计
  对账仍属于 P1，缺测区间仍必须保持不可计费，禁止按零用量结算。

- **Metering A315 immutable daily sampled rollup（2026-07-20）**：关闭 A314 小时
  证据之上的确定性跨日采样积分，不重新查询可变 Prometheus 历史。对象执行器新增
  `blob-read`：按 exact key 分页枚举 version，要求恰好一个 version 且没有 delete
  marker；随后按 exact version ID 核对 format、artifact ID、instance、store、大小、
  SHA-256 metadata、Object Lock mode/retain-until，下载响应还必须返回同一 version ID，
  字节数和重新计算的 digest 必须匹配，才原子发布本地输入和
  `kubebrain.object-immutable-blob-read.receipt.v1`。这避免 S3 “latest object”语义、
  覆盖版本或本地输出冲突进入计费证据链。

  `kubebrain-metering-rollup` 每日 UTC 00:47 处理前一完整 UTC 日，严格读取 24 个连续
  小时槽。`kubebrain.metering-rollup.v1` 固定内嵌每个源的 key、version ID、digest、
  bytes 和 retain-until；任一缺槽、重复/乱序槽、实例错配、非 canonical sample、
  指标顺序变化、源保留不足或 receipt/下载字节不一致都会在写汇总前 fail closed。
  CPU、内存、网络收发及 provisioned/used storage 采用明确的小时末样本保持契约，
  `quantity = Σ(value × 3600)`，输出 core-seconds、bytes 或 byte-seconds；备份大小和
  age 仅保留 min/max/last，不伪装成对象存储费用。CPU/网络源仍是 5 分钟 rate，因此
  这是可重放的约定采样积分，不声称等于底层 counter 的精确 increase。

  日汇总继续经 Object Lock 条件上传，retain-until 固定为最早小时源的保留截止点，
  防止汇总引用先过期的源。默认运行与显式
  `--period-end-unix=<aligned UTC boundary>` 回补使用相同 artifact ID/object key；
  只允许已超过 finalization delay 的完整历史日，未来、未对齐或未完成周期直接拒绝。
  生产 CronJob 使用独立 Secret、无 ServiceAccount token、非 root、只读根文件系统，
  其对象身份应只允许读 sample prefix 和写 rollup prefix。

  对象存储包普通测试通过（0.175 秒），确定性连续 100 轮通过（12.442 秒），race
  连续 20 轮通过（14.770 秒）；计量包普通测试通过（0.083 秒），确定性连续 100 轮
  通过（4.591 秒），race 连续 20 轮通过（4.408 秒）。两个模块 vet、根模块全量
  `go test ./...`/`go vet ./...`、production manifest 测试和 Kubernetes client
  dry-run 均通过。真实 MinIO Object Lock 集成写入完整 24 小时样本并连续执行两次
  日汇总，源码执行器 1.99 秒、最终镜像执行器 3.30 秒通过；独立 `mc` 清点正好得到
  24 个 sample version 和一个 9,336-byte rollup version，全部 versionOrdinal=1、
  无 delete marker。汇总 exact version 的 format、artifact ID、SHA-256 checksum/
  metadata、COMPLIANCE retain-until 均匹配；临时 NodePort 和检查 Pod 已删除，锁定
  测试对象按保留策略到期。

  发布镜像 `kubebrain:a315-metering-rollup` 由完整代码提交
  `4cede7ca741408ff200ea407e09aa875b6e07e8d` 构建，本地镜像 digest 为
  `sha256:c488d29a1dcab5e68411281ec39dcf927de8e23241a7bf729285bcb7f8afe4e1`；
  OCI revision/version、rollup backfill 参数和镜像内对象执行器均已核对。一次错误
  SHA 的构建在生成镜像前被中止，没有进入发布。A314 三副本 StatefulSet 有序滚动到
  A315 后 3/3 Ready、restartCount=0，PD/TiKV 3+3 Ready，etcdctl Status 保持 3.7.0，
  endpoint proposal health 和 production release gate 全部通过。跨日采样积分至此
  完成；价格版本、底层 counter 精确 increase 策略、对象存储成本和账单审计对账仍属
  P1，不能据此宣称最终 billing 已完成。

- **Metering A316 counter-based hourly contract v2（2026-07-20）**：修复 A315 已明确
  保留的 CPU/网络 5 分钟 rate 小时末外推误差。保留 v1 recording rules 和 immutable
  artifact 语义不变，新增 `kubebrain_dbaas:metering_hour_complete`：过去一小时的
  `metering_data_complete` 最小值必须为 1，并且至少有 60 个一分钟 evaluation 样本，
  防止规则刚部署时用短窗口冒充完整小时。新增 CPU 原始 counter `increase[1h]`
  core-seconds、网络收发 counter `increase[1h]` bytes，以及内存和
  provisioned/used storage 的完整小时平均值；全部继续受小时完整性 gate 约束。

  `kubebrain.metering-sample.v2` 固定采集上述 6 个小时资源量/均值和两项备份观测。
  `kubebrain.metering-rollup.v2` 对 CPU core-seconds 和网络 bytes 直接跨槽求和，对
  内存和存储 hour_avg 乘 3600 生成 byte-seconds。Prometheus `increase` 的 counter
  reset 处理和 scrape 边界外推是该版本测量契约的一部分，后续价格版本不得静默改用
  其他公式。备份 artifact/age 仍只产生 min/max/last，不当作对象存储费用。

  升级日可包含 v1/v2 混合小时：generic `blob-read` 从单 format 改为最多 8 项、非空、
  无重复的显式 allowlist；远端 metadata 必须命中 allowlist，receipt 返回实际 format。
  Roller 只允许已知 v1/v2，把每个 source 的 artifact format 固定进 v2 rollup；v1
  CPU/网络按旧 rate×3600，v2 按小时 increase 直接累加，未知或 format/content 不一致
  均拒绝。由此不需要覆盖旧对象，也不会在升级日混用公式而不留证据。

  官方 Prometheus `promtool v3.5.0 check rules` 对完整 51 条生产规则通过；当前 kind
  集群未安装 Prometheus Operator CRD，因此本轮不把 Kubernetes dry-run 或真实一小时
  Prometheus evaluation 冒充已执行证据。计量包普通测试通过（0.078 秒），确定性
  连续 100 轮通过（4.713 秒），race 连续 20 轮通过（4.440 秒）；对象存储普通测试
  通过（0.168 秒），确定性连续 100 轮通过（12.599 秒），race 连续 20 轮通过
  （14.595 秒）。两个模块 vet、根模块全量测试/vet 和 production manifest 测试通过。
  单测覆盖 60 点门禁的规则文本、v1/v2 指标顺序、混合升级日公式、format allowlist
  和 canonical source format。

  真实 MinIO Object Lock v2 集成连续两次读取 24 个小时 source 并归档同一日 rollup，
  源码执行器 2.01 秒、最终镜像执行器 2.02 秒通过；独立 `mc` 清点正好 25 个 version，
  全部 versionOrdinal=1。10,572-byte rollup 的 format 为
  `kubebrain.metering-rollup.v2`，SHA-256 checksum/metadata、exact version 和
  COMPLIANCE retain-until 均匹配；临时 NodePort/检查 Pod 已删除。

  发布镜像 `kubebrain:a316-counter-metering` 由完整代码提交
  `1833bf07c4c00adc9da1832430099d403c139ddf` 构建，本地镜像 digest 为
  `sha256:185922837cb38410f327ad3d12f84f1e76a64bf5892bedc1b67a7fbae24547d4`，
  OCI revision/version 和镜像内对象执行器均已核对。A315 三副本 StatefulSet 有序
  滚动到 A316 后 3/3 Ready、restartCount=0，PD/TiKV 3+3 Ready，etcdctl Status
  保持 3.7.0，endpoint proposal health 和 production release gate 全部通过。
  counter-based 小时测量与混合格式迁移至此完成；价格版本、对象存储成本和账单审计
  对账仍属 P1。真实 Prometheus 一小时 evaluation 应在安装 Operator 的预生产监控
  集群作为额外发布门禁补跑。

- **Metering A317 不可变价格版本与资源计费（2026-07-20）**：新增
  `kubebrain.metering-price-catalog.v1`，将稳定版本、三位大写币种、有效期和固定
  `kubebrain.metering-rollup.v2` 策略绑定到 6 项有序资源单价。单价必须是无指数、
  无多余零且最多 18 位小数的 canonical decimal；Publisher 在发起任何 S3 请求前
  完成完整 canonical 校验，并按 `scope/version.json` 写入 Object Lock。

  新增 `kubebrain.metering-charge.v1`。Biller 依次读取并独立校验 exact-version
  rollup 与 price catalog receipt、下载字节和 SHA-256，且价格有效期必须完整覆盖
  计费周期。计算先把十进制精确转换为 `big.Rat`，再逐资源行执行
  `half_even_to_currency_micro.v1` 银行家舍入并检查 int64 溢出，总额是已舍入行之和。
  charge 固化两类 source 的 format/artifact ID/object key/version/SHA/bytes/retain
  证据。同一实例和周期只有一个 charge object key，价格版本不参与 key；因此第二个
  价格版本不能静默重算同一周期，未来纠错必须使用独立 adjustment/credit artifact。
  CronJob 默认在 UTC 01:17 对前一完整日计费，也提供显式 period-end 历史回填。

  billing 包普通测试通过（0.024 秒），确定性连续 100 轮通过（0.762 秒），race
  连续 20 轮通过（2.142 秒）；focused vet、根模块全量测试/vet、production manifest
  测试和 client dry-run 均通过。单测覆盖 half-even 边界、价格覆盖区间、固定顺序、
  canonical decimal/source、篡改和溢出拒绝、Publisher 先校验后访问对象存储，以及
  Biller 的 fail-closed 读取顺序。

  真实 MinIO Object Lock 集成由实际 Publisher 写入 v1 catalog，再由 Biller 对锁定
  v2 rollup 生成 charge；重复执行返回同一 receipt，发布 v2 catalog 后尝试重算同一
  周期被不可变 charge 冲突拒绝。源码执行器测试曾以 0.35 秒通过，最终 A317 镜像内
  对象执行器复验以 4.58 秒通过。独立对象清点得到 4 个 version，全部
  versionOrdinal=1；1,765-byte charge 的 format、SHA-256 metadata/checksum、exact
  version 和 COMPLIANCE retain-until 均匹配。临时 NodePort/检查 Pod 已删除，锁定
  测试对象按保留策略到期。

  发布镜像 `kubebrain:a317-metering-prices` 由完整代码提交
  `cbe08d1dec4c4f39616c58f72d642c9df97f0702` 构建，本地镜像 digest 为
  `sha256:ca8d1cab91cf99aa8b9c6ff32007ec632dc7fbe90be6a4097b466e9ef488ffb1`；
  OCI revision/version 及镜像内 charge、price-publish 和对象执行器均已核对。A316
  三副本 StatefulSet 有序滚动到 A317 后 3/3 Ready、restartCount=0，PD/TiKV 3+3
  Ready，endpoint proposal health 和 production release gate 全部通过。不可变资源
  价格版本和 charge 至此完成；对象存储保留成本、adjustment/credit、最终 invoice
  与审计对账仍属 P1。真实 Prometheus 一小时 evaluation 仍须在安装 Operator 的
  预生产监控集群补作发布证据。

- **Metering A318 exact-version 对象存储保留量计费（2026-07-20）**：关闭 A317
  `logical_backup_artifact_bytes:last` 不能表示全部保留 version 的缺口。nested
  objectstore 新增 `kubebrain.object-usage.receipt.v1`：对实例独占 prefix 完整分页
  `ListObjectVersions`，拒绝 delete marker、重复/非法 identity 和未前进分页，再逐
  exact version 核对 allowlist format、store ID、Head/list bytes、artifact SHA
  metadata、Object Lock mode/retain-until。排序后的完整 version evidence 产生
  `versions_sha256`，总字节使用 int64 溢出保护；未知 format、metadata/retention 漂移
  或控制面漏报均 fail closed。

  每小时锁定 `kubebrain.object-storage-sample.v1`，绑定 source store/bucket/prefix、
  allowlist、version count、total bytes、versions digest 和 checked-at。source
  inventory 与 metering evidence 使用两套显式 endpoint/credential 子进程环境和独立
  Secret，且均不挂载 ServiceAccount token。一次审查发现生产 Cron 在每小时第 27
  分钟运行，却把 10 分钟 eligibility delay 误作 checked-at 最大窗口，会稳定拒绝正常
  采样；修复后 10 分钟只选择完整小时，采集/重试证据窗口固定 45 分钟，Roller 再次
  校验该窗口并有第 27 分钟回归测试。

  日 Roller 只读取连续 24 个 exact snapshot，scope/allowlist 必须整日不变，输出
  `kubebrain.object-storage-rollup.v1`。计量策略明确为小时末离散持有量
  `sum(total_object_bytes * 3600)`，不是对象事件的连续积分；历史时点不能用当前
  inventory 伪造回填。`kubebrain.metering-price-catalog.v2` 在原 6 项后追加
  `object_storage_byte_seconds/byte_seconds`，measurement policy 固定为
  `kubebrain.metering-rollup.v2+object-storage-rollup.v1`。Biller 显式固定 catalog
  format 后读取 resource/storage 两份 rollup，生成含三份 exact-version source 和
  7 行金额的 `kubebrain.metering-charge.v2`；v1 继续可读但禁止混入 storage source，
  同周期 charge key 继续阻止静默重定价。

  storage/billing 普通测试通过；确定性连续 100 轮分别 1.123/1.095 秒，storage race
  连续 20 轮 2.041 秒。nested objectstore race 20 轮 14.479 秒、确定性 100 轮
  12.708 秒；根模块全量测试（production 包 91.156 秒）、根/nested vet、manifest
  测试、client dry-run 和 diff check 均通过。测试覆盖完整分页、delete marker、
  metadata/retention/scope 漂移、24 槽连续性、缺槽、digest/bytes 篡改、溢出、
  两套凭据隔离、v1/v2 source 约束和缺 storage rollup 时零归档。

  真实 MinIO Object Lock 先锁定一个 3,430-byte logical source，再由实际 usage 和
  storage archiver 生成 448-byte snapshot；exact-version 下载证明 remoteVersions=1、
  deleteMarkers=0、totalObjectBytes=3,430，和源对象字节完全一致。实际 Publisher、
  resource/storage rollup 与 v2 Biller 的完整链路以源码 executor 1.85 秒、最终镜像
  executor 0.79 秒通过；重复计费返回同一 receipt，第二个 v2 价格版本对同周期重算被
  immutable charge 冲突拒绝。临时 NodePort 已删除，测试对象按 COMPLIANCE 到期。

  发布镜像 `kubebrain:a318-storage-metering` 的代码提交为
  `89e383d0d90799357dd544fc4e3b17547db6ebbf`，本地 digest 为
  `sha256:de340ef2bab7614c1d8f0a2a96bf99e5afec7c6524f24590ff6269a8efdb549f`；
  OCI revision/version 和镜像内 storage-archive、storage-rollup、v2 charge 及对象
  执行器均已核对。A317 三副本 StatefulSet 有序滚动到 A318 后 3/3 Ready、
  restartCount=0，PD/TiKV 3+3 Ready，endpoint proposal health 和 production release
  gate 全部通过。对象 storage byte-time 资源 charge 至此完成；请求费、税费、折扣、
  adjustment/credit、最终 invoice/审计对账仍属 P1，真实连续 24 小时采样还应在
  预生产验证后再启用 v2 catalog。

- **Metering A319 approved adjustment 与 final invoice（2026-07-20）**：不再用覆盖
  charge 的方式纠错。新增 `kubebrain.metering-adjustment.v1`，用非零 signed
  amount-micros 表示补收或 credit，固定 reason code、单日 charge 周期/币种和该
  charge 的完整 exact-version source。adjustment 必须携带专用 billing approver
  身份、外部 approval ID、不得早于周期结束且不得晚于发布时刻的审批时间，并在批准时
  绑定唯一 invoice ID。一次实现审查发现只保证 plan 内 adjustment ID 唯一仍允许两张
  invoice 重复抵扣同一 credit；invoice ID 交叉绑定和回归测试关闭了该漏洞。

  新增 `kubebrain.metering-invoice-plan.v1` 作为不可变结算集合边界。plan 固定 invoice
  ID、实例、UTC 账期、币种、逐日连续且有序的 charge format，以及排序无重复的
  adjustment ID；finalizer 不列举 prefix，因此迟到对象不能静默进入已审批账期。专用
  publisher 在任何 S3 请求前执行 strict/canonical schema、审批时间和保留期校验。
  同 plan/adjustment ID 不同内容只能触发 Object Lock 冲突。

  `kubebrain.metering-invoice.v1` 先固定 plan exact version，再按 plan 顺序读取并独立
  校验每个 charge/adjustment receipt、下载字节、digest、format/ID/instance/period/
  currency/retention。每项 adjustment 的 charge source 必须与对应日实际 charge
  receipt 完全相同，且 invoice ID 必须等于 plan ID。subtotal 与 signed adjustment
  total 使用 int64 溢出保护，最终 total 不允许为负；plan approval timestamp 固定为
  finalized-at，使崩溃重试逐字节确定。invoice 内嵌 plan、全部 charge 和 adjustment
  exact-version source 与可重算金额。

  billing 普通测试、根模块全量测试/vet、manifest 测试、client dry-run 和 diff check
  全部通过；确定性连续 100 轮 1.657 秒、race 连续 20 轮 3.637 秒。覆盖 forged/future
  approval、非连续 charge、重复 adjustment、币种/周期/source 错配、signed total
  溢出、负 invoice、非 canonical 篡改、publisher 校验前零 S3、plan→charge→adjustment
  固定读取顺序、缺源零归档和两次 finalization 字节一致。

  真实 MinIO Object Lock 链路依次锁定 v2 charge、approved adjustment、approved
  invoice plan 和 final invoice；两次 finalize 返回同一 receipt，修改同 plan ID 内容
  被不可变冲突拒绝。源码 executor 1.38 秒、最终镜像 executor 2.02 秒通过；临时
  NodePort 已删除，测试对象按 COMPLIANCE 到期。

  发布镜像 `kubebrain:a319-invoice-settlement` 由代码提交
  `9eb8f46226c11345f49e412ec3b254465c6c9cf7` 构建，本地 digest 为
  `sha256:490740d748aad4b20aa824ef15c6b20ce790fcb326f8997fe7f7934da5ce361a`；
  OCI revision/version 和镜像内 settlement-publish、invoice-finalize 与对象执行器
  均已核对。一次短 SHA 构建在生成镜像前被 metadata gate 拒绝。A318 三副本
  StatefulSet 有序滚动到 A319 后 3/3 Ready、restartCount=0，PD/TiKV 3+3 Ready，
  endpoint proposal health 和 production release gate 全部通过。

  不可变 adjustment/credit 和数据面 final invoice 至此完成，但它不是完整税务/收款
  系统。对象请求费、税率计算、折扣规则、付款/退款、应收账款、法规发票编号、外部
  总账过账及跨账户财务对账仍属 P1；`tax_correction` 只能承载外部系统已批准的结果。

- **Metering A320 对象请求费与 charge v3（2026-07-20）**：请求成本不再从本地
  executor 调用或重试推断。定义供应商/网关 exporter 输入合同：
  `kubebrain_object_store_request_count` 必须按实例和 `write/list/read/delete` 四类
  输出前一完整小时的 finalized 非负整数，`kubebrain_object_store_request_period_end_seconds`
  显式绑定该小时结束时间。recording rules 对五个源分别要求恰好一条，并用
  `or on() vector(0)` 让完全缺失仍触发 critical 告警；完整 60 次 evaluation 后才
  转发请求 gauge。分类映射负责纳入供应商实际收费的重试、复制和生命周期请求，数据面
  不猜测不同供应商 API 的计价类别。

  `kubebrain.metering-sample.v3` 在 v2 八项后追加四类请求量并内嵌 period-end，要求
  与 slot end 精确相等、值为 `0..2^53` 的整数。24 个源全部为 v3 才生成
  `kubebrain.metering-rollup.v3`；任一旧格式源都会生成不含请求量的 v2 rollup，避免
  升级日被静默少收。v3 rollup 自身要求全部 source format 为 sample v3，并对四项
  日总量再次执行整数/上限校验，关闭伪造 canonical rollup 绕过构造器的路径。

  新增 `kubebrain.metering-price-catalog.v3`，measurement policy 固定为
  `kubebrain.metering-rollup.v3+object-storage-rollup.v1`，11 项 rate 顺序为六项资源、
  write/list/read/delete 和 storage byte-seconds。`kubebrain.metering-charge.v3`
  固定三份 exact-version source 和 11 行可重算金额；独立读取时仍拒绝小数请求量。
  settlement adjustment、plan 和 invoice allowlist 同步接受 v3，v1/v2 历史证据保持
  可读且不改写。生产 charge ConfigMap 默认切到 v3，但没有完整 v3 UTC 日与 approved
  catalog 时必须 fail closed。

  根模块 `go test ./...`、`go vet ./...`、manifest 测试和 diff check 通过；核心包
  确定性 100 轮分别 5.071/2.041 秒，race 20 轮分别 4.838/4.247 秒。专项覆盖请求
  completeness 缺失、错 period、小数/超 `2^53`、v3 rollup 引用 v2 source、v2/v3
  混合升级日、v3 catalog/rollup 代际错配、伪造 charge 小数和固定 11 行顺序。
  当前 kind 未安装 Prometheus Operator CRD，因此 `monitoring.yaml` 的 client dry-run
  按预期无法解析 `ServiceMonitor/PrometheusRule`；结构和完整 PromQL 由 manifest
  测试固定，真实一小时 evaluation 仍保留为预生产门禁。

  源码 executor 和最终镜像 executor 分别对独立 MinIO Object Lock bucket 完成真实
  v3 链路：锁定 resource/storage rollup 与 catalog，连续两次生成同一 charge/receipt，
  再以同 charge key 不同 approved 价格验证 immutable conflict；分别 0.44/0.48 秒。
  发布镜像 `kubebrain:a320-object-request-metering` 从干净代码提交
  `8327eab27ed5d29ce9b2b656c99b9dc8380973fa` 构建，本地 digest 为
  `sha256:62ec7246b31d7346263b8d7f876caca04cf7eb7bd04b4cde920351c34800262c`，
  OCI revision/version 和镜像内 archive/rollup/charge/object executor 已核对。A319
  三副本 StatefulSet 有序滚动到 A320 后 3/3 Ready、restartCount=0，独立 PD/TiKV
  3+3 Ready，endpoint proposal health 19.78ms 和 production release gate 均通过。

  数据面对象请求 charge 至此完成；供应商分类 exporter 与供应商账单周期对账、税率/
  折扣、付款/退款、应收账款、法规发票编号、外部总账及跨账户财务对账仍属 P1。

- **KV A321 空键 namespace 与差分桥接可靠性（2026-07-20）**：继续对照
  `/root/etcd/tests/integration/v3_kv_test.go:TestKVWithEmptyValue`，新增官方
  `client/v3/namespace` 双端差分。普通空键 Delete 必须精确返回
  `InvalidArgument: etcdserver: key is not provided`；同一个空业务键配合
  `WithFromKey` 经 namespace 前缀转换后则必须能列出并删除该 namespace 内全部
  对象，且不能越过 64-byte `0xff` 隔离前缀。参考 etcd 3.7 与真实
  TiKV-backed KubeBrain 的错误 code/message、可见 key、删除数和残留数连续 10 轮
  一致，race 连续 3 轮通过。本轮未发现数据面实现差异，新增门禁防止后续空键校验
  错误地发生在 namespace 转换之后。

  全量差分首次使用 clientv3 可接受的 `http://host:port` 参考地址时，暴露共享
  `tcpBridge` 直接把 URL 交给 `net.DialTimeout`：桥接永远连接失败，
  Get cancel 用例等待 30 秒，后续 leasing 初始化又在无 context 的
  `leasing.NewKV` 中等到全套 10 分钟超时。桥接器现统一去除 `http://`/`https://`
  scheme，并用 bare/http/https 三组单元测试固定 dial target；带 scheme 的 Get
  cancel、ambiguous leasing 和空键场景分别以 0.36/8.80/0.81 秒通过。完整 suite
  仍按脚本规定的裸 `host:port`（部分 raw gRPC 测试的显式契约）执行，最终
  362.817 秒通过。

  根模块 `go test ./...`、根/compat `go vet ./...`、桥接 normalization race
  连续 10 轮和 `git diff --check` 均通过。在线 StatefulSet 仍运行已提交源码构建的
  `kubebrain:a320-object-request-metering`，3/3 Ready、restartCount=0；独立 PD/TiKV
  均 3/3 Ready，production release gate 与 32.08ms endpoint proposal health 通过。
  A321 仅增强测试代码、未改变数据面运行路径，因此不制造新镜像或无意义滚动发布。

- **Compatibility A322 raw gRPC endpoint normalization（2026-07-20）**：A321
  完整差分诊断使用 `http://host:port` 时，clientv3 场景和 TCP bridge 修复后可正常
  建连，但 `TestStreamRequestLimit` 与 `TestWatchControlDifferential` 仍在约
  430 秒后把完整 URL 直接交给 `grpc.NewClient`，以 `too many colons in address`
  失败。该差异来自测试传输入口而非数据面：同一套 suite 中已有约 30 个 raw gRPC
  helper 各自 TrimPrefix，另有 admission、request-rate、stream-limit、watch
  control/quota 和 lease keepalive cancel 等入口没有处理，导致 endpoint 表达形式
  影响发布结论。

  A321 的 bridge-only helper 提升为 package 级 `grpcTarget`，统一处理 bare、
  `http://` 和 `https://` target；全部尚未标准化的 raw gRPC/health/watch 入口和
  TCP bridge 共同调用它。带 scheme 的 StreamRequestLimit、WatchControl、Get
  cancel、ambiguous leasing 与 helper 回归连续 3 轮 31.873 秒通过，focused race
  2.886 秒通过；最终参考 etcd 与真实 TiKV-backed KubeBrain 均使用
  `http://...` 的完整 172-test compat suite 以 344.220 秒通过，证明修复覆盖全套
  而非只绕过首个失败。

  同期复核已知 revision 跳号差异：当前 `BatchWrite` 只支持调用者预先给出期望值
  的 CAS，不能在同一事务中读取全局 revision、分配 next 并把结果返回给对象键编码；
  TSO 又必须在存储提交前分配 revision，失败后通过 invalid event 推进 collector。
  因此删除 invalid event 或复用失败 revision 会破坏并发唯一性、uncertain commit
  解析和 watch 连续推进。正确方案需新增存储层事务内 read-modify-write revision
  原语，并同时重构所有写路径、event log 和 uncertain resolution，不能用 leader-local
  序列化假装关闭跨 leader 差异。

  根模块 `go test ./...`、根/compat `go vet ./...` 和 `git diff --check` 全部通过。
  在线 `kubebrain:a320-object-request-metering` production release gate 通过，
  KubeBrain 3/3、PD/TiKV 3+3 Ready，endpoint proposal health 28.40ms。A322
  只修测试门禁，不改变服务二进制，因此不构建镜像或滚动 StatefulSet。

- **Watch A323 mixed PrevKV 独立 stream 隔离（2026-07-20）**：对照
  `/root/etcd/tests/integration/clientv3/watch/watch_test.go` 的
  `TestWatchMixedPrevKVOnSameKeySeparateStreams` 补齐双端黑盒。6 个 watcher 通过不同
  outgoing metadata 强制建立独立 gRPC watch stream，其中 3 个请求 `WithPrevKV`、
  3 个明确不请求；同一 key 连续更新 8 次后，每轮同时核对全部 stream 的当前
  key/value、带 PrevKV stream 的精确前值，以及不带 PrevKV stream 必须保持
  `PrevKv=nil`。

  该场景约束 KubeBrain 共享 backend event 的所有权：`watch.go` 不能为不请求前值的
  watcher 原地清除共享 protobuf event，否则其他较慢 stream 会随机丢 PrevKV；当前
  `withoutWatchPrevKvs` 为每个 event 构造 field-level shallow copy，只清除副本字段。
  参考 etcd 3.7 与真实 TiKV-backed KubeBrain 连续 10 轮（每端每轮 48 个事件）
  15.004 秒通过，race 连续 3 轮 1.931 秒通过，未发现跨 stream 污染或服务实现差异。

  带 scheme 的完整 compat suite 346.268 秒通过；根模块 `go test ./...`、根/compat
  `go vet ./...` 与 `git diff --check` 均通过。在线
  `kubebrain:a320-object-request-metering` release gate 保持 KubeBrain 3/3、
  PD/TiKV 3+3 Ready，endpoint proposal health 29.67ms。A323 只增加永久兼容门禁，
  不改变服务二进制，因此不构建镜像或滚动发布。

- **Auth A324 LeaseLeases 全局关联键授权（2026-07-20）**：对照
  `/root/etcd/tests/common/auth_test.go` 的 `TestAuthLeaseLeases` 扩展可销毁实例双端
  auth 差分。认证启用后，匿名 `LeaseLeases` 必须返回 `ErrUserEmpty`；当任一存活租约
  关联普通用户无权读取的 key 时，普通用户列举全部租约必须整体返回
  `PermissionDenied`；root 必须看见目标 lease。root 撤销该越权 lease 后，同一普通
  用户调用立即恢复成功。断言同时记录语义错误标志，避免把上游当前 gRPC `Unknown`
  映射误写成稳定错误码契约。

  差分夹具清理同步改为可重入：关闭 auth 后先枚举用户并撤销其全部角色，再删除用户、
  角色和测试 key，解决 root 仍绑定 root 角色时无法删除的残留。参考 etcd 与运行当前
  源码、使用独立 `a324-auth-2` keyspace 的真实 TiKV-backed KubeBrain 首轮 5.74 秒
  通过；相同端点连续 3 轮 17.119 秒、race 2 轮 13.171 秒通过。最终确定性清理又在
  两个全新参考 etcd 上连续 3 轮 9.024 秒通过。

  带 scheme 的完整 172-test compat suite 381.888 秒通过；根模块
  `go test ./...`、根/compat `go vet ./...` 与 `git diff --check` 均通过。在线
  `kubebrain:a320-object-request-metering` production release gate 保持 KubeBrain
  3/3、PD/TiKV 3+3 Ready，endpoint proposal health 30.22ms。A324 未发现服务端
  实现差异，只增加永久兼容门禁与确定性测试回收，因此不构建镜像或滚动发布。

- **Auth A325 LeaseTimeToLive attached-key 信息边界（2026-07-20）**：对照
  `/root/etcd/tests/common/auth_test.go` 的 `TestAuthLeaseTimeToLive` 扩展可销毁实例
  双端差分。匿名调用无论是否请求 attached keys 均必须返回 `ErrUserEmpty`；普通用户
  对关联越权 key 的 lease 查询不带 keys 时可读取 TTL，但 `WithAttachedKeys` 必须
  返回 `PermissionDenied`，防止泄露无权读取的 key 名；root 带 keys 查询必须精确
  看见受保护 key。该矩阵复用 A324 的受保护 lease，不额外改变 auth 生命周期。

  参考 etcd 与运行当前源码、使用独立 `a325-auth` keyspace 的真实 TiKV-backed
  KubeBrain 首轮 6.13 秒通过；相同端点连续 5 轮 30.463 秒、race 2 轮 14.115 秒
  通过。带 scheme 的完整 172-test compat suite 343.983 秒通过；根模块
  `go test ./...`、根/compat `go vet ./...` 与 `git diff --check` 均通过。在线
  `kubebrain:a320-object-request-metering` production release gate 保持 KubeBrain
  3/3、PD/TiKV 3+3 Ready，endpoint proposal health 34.05ms。A325 未发现服务端
  实现差异，只增加永久信息泄露边界门禁，因此不构建镜像或滚动发布。

- **Auth A326 keepalive stream 动态撤权（2026-07-20）**：在可销毁实例双端 auth
  差分中补齐长连接逐请求鉴权。普通用户创建 lease 并绑定授权 key 后，通过 raw
  `LeaseKeepAlive` stream 的首个请求必须成功；root 撤销该 key-range 权限后，同一
  stream 的下一请求必须立即返回 gRPC `PermissionDenied`，不能沿用建流时的授权
  快照；恢复权限后新 keepalive 请求必须再次成功。该场景扩展了上游
  `TestAuthLeaseKeepAlive` 的一次性 root 验证，并为 KubeBrain 已有的逐消息鉴权实现
  建立真实 TiKV 黑盒门禁。

  raw generated gRPC client 返回标准 `codes.PermissionDenied`，但不会像 clientv3
  一样包装为可被 `errors.Is(..., rpctypes.ErrPermissionDenied)` 识别的错误；差分
  错误归类因此统一接受标准 gRPC code 或 rpctypes 语义匹配，避免入口不同造成假阴性。
  参考 etcd 与使用独立 `a326-auth-2` keyspace 的真实 TiKV-backed KubeBrain 首轮
  6.73 秒通过；相同端点连续 5 轮 33.984 秒、race 2 轮 15.466 秒通过。

  带 scheme 的完整 172-test compat suite 349.346 秒通过；根模块
  `go test ./...`、根/compat `go vet ./...` 与 `git diff --check` 均通过。在线
  `kubebrain:a320-object-request-metering` production release gate 保持 KubeBrain
  3/3、PD/TiKV 3+3 Ready，endpoint proposal health 32.67ms。A326 未发现服务端
  实现差异，只增加永久长连接授权门禁，因此不构建镜像或滚动发布。

- **Auth A327 multiplex Watch 动态撤权边界（2026-07-20）**：在可销毁实例双端
  auth 差分中补齐同一 raw gRPC Watch stream 的角色变更状态机。普通用户先成功创建
  watch；root 撤销其 key-range 权限后，etcd 的既有 watch 不被追溯取消，仍可收到
  root 对该 key 的后续事件，但同一 multiplex stream 上的新 WatchCreate 必须返回
  canceled response 和 `etcdserver: permission denied`。恢复权限后，该 stream 必须
  可再次创建 watch，下一事件同时 fan-out 给旧、新 watch ID。

  该契约明确区分建流身份与 logical WatchCreate 授权：服务端必须逐个鉴权新建请求，
  拒绝一个 logical watch 不能终止 multiplex stream；同时 KubeBrain 不应擅自强化为
  撤权即取消已有 watch，否则会偏离 etcd 的长连接行为。参考 etcd 与使用独立
  `a327-auth` keyspace 的真实 TiKV-backed KubeBrain 首轮 7.24 秒通过；相同端点
  连续 5 轮 35.812 秒、race 2 轮 17.598 秒通过。

  带 scheme 的完整 172-test compat suite 396.715 秒通过；根模块
  `go test ./...`、根/compat `go vet ./...` 与 `git diff --check` 均通过。在线
  `kubebrain:a320-object-request-metering` production release gate 保持 KubeBrain
  3/3、PD/TiKV 3+3 Ready，endpoint proposal health 39.08ms。A327 未发现服务端
  实现差异，只增加永久 Watch 授权状态机门禁，因此不构建镜像或滚动发布。

- **Capacity A328 tenant logical quota / NOSPACE（2026-07-20）**：新增
  `--quota-backend-bytes`（默认 0 禁用），按 keyspace 原子计量当前存活用户
  key+value 逻辑字节。用量元数据与用户 MVCC、event log 在同一 TiKV transaction
  CAS 提交，因此多副本和换主后保持一致；首次启用由 leader 在 readiness 前独占扫描
  存量数据并 PutIfNotExist 初始化，初始化失败时 fail closed，不会把存量误算为 0；
  若存量已超过新上限，readiness 前立即持久激活 NOSPACE。

  正增长超过上限时返回标准 etcd `ResourceExhausted / mvcc: database space exceeded`
  并持久激活 NOSPACE；拒绝发生在分配 revision 前。A329 对照上游后纠正告警期行为：
  sticky alarm 激活后所有 Put（包括缩小 value）及任一分支含 Put 的 Txn 均拒绝；
  删除和 lease revoke 仍可释放容量。`Alarm(DEACTIVATE, NOSPACE)` 可按 etcd
  契约随时解除；若用量仍达到 quota，下一次含 Put 请求会立即重新激活。`Status`
  报告逻辑用量与配置 quota，新增
  `quota.logical_usage_bytes`、`quota.backend_bytes`、`quota.nospace` 指标。该数字不
  等同 bbolt 文件或 TiKV 物理磁盘占用，后者继续由 PD/TiKV 指标管理。

  backend 单测覆盖 exact-limit、拒绝不消费 revision、sticky alarm、缩容恢复、禁用
  行为和存量数据一次性初始化；RPC 单测覆盖 Status、Put/Txn NOSPACE、Alarm
  list/disarm、Delete 恢复和 LeaseGrant 阻断。聚焦 backend/server race 各 3 轮、
  根模块 `go test -count=1 ./...`、根/compat `go vet ./...` 和完整生产 Dockerfile
  构建均通过。

  `kubebrain:a328-tenant-quota` 在独立 `a328-quota` keyspace、真实 3 PD/3 TiKV 上
  验证：64 B exact-limit 成功，继续增长返回 NOSPACE，alarm list、LeaseGrant 阻断、
  Delete/disarm/恢复写、usage/quota/nospace 指标均正确；Pod 重建后 11 B 用量、数据
  和已解除 alarm 状态保持。相同镜像的独立 `a328-compat` keyspace 完整差分套件
  310.952 秒通过。临时资源已清理；在线
  `kubebrain:a320-object-request-metering` 保持 3/3 Ready、restartCount=0，
  PD/TiKV 3+3 Ready，production release gate 与 44.116ms endpoint proposal
  health 通过。

- **Capacity A329 sticky NOSPACE capped write state（2026-07-20）**：对照
  `/root/etcd/server/etcdserver/apply/capped.go` 的 capped applier 与
  `BackendQuota.Cost` 行为，修复 A328 告警期仍允许缩小 value 的偏差。持久
  NOSPACE 激活后，普通 Put 无论逻辑用量 delta 是否为负均返回标准
  `ResourceExhausted`；Txn 在执行 compare 前检查 success、failure 及嵌套 Txn，
  只要任一分支包含 Put 就整体拒绝，因此未选中分支不能绕过 capped state。
  delete-only/read-only Txn 与 LeaseRevoke 保持可用，LeaseGrant 继续拒绝，释放容量后
  必须显式 disarm 才恢复写入。

  Maintenance API 新增 root-only `Alarm(ACTIVATE, NOSPACE)`，用于管理面和故障注入；
  quota 未配置时 fail closed 为 `FailedPrecondition`，成功 activate/deactivate 均返回
  当前 member 的 NOSPACE `AlarmMember`。CORRUPT 和其他 alarm mutation 仍返回
  `Unimplemented`。backend 层也在 TiKV transaction 提交前读取持久 alarm，避免绕过
  RPC 层的内部写路径破坏该状态。

  新增 raw gRPC 双端差分，逐项核对手工 activate、缩小 Put、未选中 Put 分支、
  delete-only/read-only Txn、LeaseGrant、deactivate 和恢复写。参考 etcd 与
  `kubebrain:a329-nospace-cap` 在真实 3 PD/3 TiKV、独立 `a329-quota` keyspace
  首轮通过，连续 5 轮 1.876 秒、race 3 轮 2.328 秒通过；相同镜像在全新
  `a329-compat-2` keyspace 的完整差分套件 272.882 秒通过。此前受并行测试争用而
  超时的 paginated mirror 与 Txn compare-header 用例，隔离后分别以 27.24 秒和
  39.20 秒通过。

  聚焦 backend/server race 各 3 轮通过；根模块串行
  `go test -p 1 -count=1 ./...`、根/compat `go vet ./...`、`git diff --check` 和完整
  生产 Dockerfile 构建通过，镜像 ID
  `sha256:d25e66602be3d0212a565c85052488f132ce25d68e6f7e3ce9adb8216a62631c`。
  在线 `kubebrain:a320-object-request-metering` release gate 保持 KubeBrain 3/3、
  PD/TiKV 3+3 Ready，endpoint proposal health 41.970ms。

- **Maintenance A330 Alarm mutation no-op / member guard（2026-07-20）**：继续对照
  `/root/etcd/server/etcdserver/apply/backend.go:Alarm`、
  `/root/etcd/server/etcdserver/api/v3alarm/alarms.go` 和
  `/root/etcd/tests/integration/v3_alarm_test.go`，补齐 NOSPACE mutation 的幂等响应。
  `ACTIVATE(NONE)`、`DEACTIVATE(NONE)` 现与 etcd 一致返回成功空列表；解除请求仅在
  member ID 匹配当前返回的 tenant alarm 时生效，错误 member 返回成功空列表且不能
  清除告警；正确解除首次返回一个 `AlarmMember`，重复解除返回空列表。

  backend `DisarmNoSpace` 状态接口现显式返回“是否移除”，RPC 不再把不存在的告警
  伪报为已解除。扩展 raw gRPC 双端矩阵到 13 步，同时比较 code 与 alarm 数量，覆盖
  NONE 空操作、activate、错误 member、错误解除后 Put 仍被 capped、缩小 Put、
  未选中 Put 分支、delete/read-only Txn、LeaseGrant、正确/重复解除和恢复写。
  参考 etcd 与 `kubebrain:a330-alarm-mutation` 在真实 3 PD/3 TiKV、独立
  `a330-quota` keyspace 首轮 0.444 秒、连续 10 轮 3.590 秒、race 3 轮 2.270 秒
  通过；独立 `a330-compat` keyspace 的完整差分套件 222.112 秒通过。

  根模块 `go test -p 1 -count=1 ./...`、聚焦 backend/server race 各 3 轮、根/compat
  `go vet ./...`、`git diff --check` 和完整生产 Dockerfile 构建均通过；镜像 ID
  `sha256:6b65ecc3a241ebd17c7f29d160cc9c0741ef2b02de7ac5ce47acd765b9ec2042`。
  两个真实 TiKV-backed Pod 均 Ready、restartCount=0 且无 panic/fatal/storage error。
  在线 `kubebrain:a320-object-request-metering` release gate 保持 KubeBrain 3/3、
  PD/TiKV 3+3 Ready，endpoint proposal health 46.229ms。

  KubeBrain 的 NOSPACE 是独立 keyspace 的单一容量状态，不伪造 etcd 可手工写入的
  “任意虚构 member ID 各自持有多条 NOSPACE alarm”存储；GET 返回当前 serving
  member 对应的 tenant alarm。该差异不影响 `etcdctl alarm list/disarm` 和自动容量
  保护，但 raw Maintenance 调用方不得依赖人为构造多 member alarm。

- **Maintenance A331 cross-serving-member alarm disarm（2026-07-20）**：三副本审计发现
  A330 的 member guard 仅接受当前 serving member，会在 `alarm list` 与后续
  `alarm disarm` 被负载均衡到不同 Pod 时把合法解除误判为空操作。对照
  `/root/etcd/server/etcdserver/v3_server.go:Alarm` 的 Raft request 路径和
  `/root/etcd/server/etcdserver/api/v3alarm/alarms.go:Deactivate`，现允许
  `MemberList` 中任一静态 KubeBrain member ID，以及动态模式中的本地/当前 leader ID
  解除 tenant-global NOSPACE；不在成员集合中的虚构 ID 仍返回成功空列表且不清告警。
  成功响应保留请求指定的 member ID，与 etcd 返回被解除 `AlarmMember` 的形状一致。

  审计同时定位到 A329/A330 的 `AlarmMember.MemberID` 在 header interceptor 执行前从
  空 header 读取，网络响应实际为 0。activate/get 现直接使用稳定的
  `localMemberID()`，返回真实非零 serving member；gRPC response header 仍由统一
  interceptor 标记。新增环境门控的三 endpoint 黑盒测试：Pod A activate，Pod B list
  并返回不同 member ID，Pod C 先用虚构 ID 验证 Put 仍被 capped，再用 Pod B 的 ID
  disarm 并确认返回 member 与恢复写。

  `kubebrain:a331-cross-member-alarm` 在真实 3 PD/3 TiKV、三个共享独立
  `a331-cross-member` keyspace 的 KubeBrain Pod 上首轮 0.315 秒、连续 20 轮
  3.505 秒、race 5 轮 2.082 秒通过；服务端聚焦 race 5 轮 2.186 秒通过。根模块
  `go test -p 1 -count=1 ./...`、根/compat `go vet ./...`、`git diff --check` 和完整
  生产 Dockerfile 构建通过；镜像 ID
  `sha256:ba2c0daec6616dc4d3c070da2a346b3d52cd0e006ea75854d1042751dd8d5786`。
  三个测试 Pod 均 Ready、restartCount=0 且无 panic/fatal/storage error。在线
  `kubebrain:a320-object-request-metering` release gate 保持 KubeBrain 3/3、
  PD/TiKV 3+3 Ready，endpoint proposal health 15.938ms。

- **Maintenance A332 persisted alarm owner / CAS disarm（2026-07-20）**：继续审计 A331
  发现其允许任一已发布 member 解除 tenant alarm，但上游
  `/root/etcd/server/etcdserver/api/v3alarm/alarms.go` 持久保存实际
  `AlarmMember`：Pod A 激活后从 Pod B GET 仍应返回 A，而不是随 serving replica
  漂移。NOSPACE 内部元数据现从单字节 active marker 升级为 8 字节 member ID；
  自动超限和手工 activate 均以 backend identity 的稳定 member ID 执行
  Put-if-absent，因此重复激活保留首个 owner。

  `NoSpaceAlarm` 在所有副本读取同一持久 owner；`DisarmNoSpace(memberID)` 在 backend
  内先核对 owner，再用 exact-value CAS 删除，错误 owner、重复解除或读取后被替换的
  告警均为空操作，不再存在 RPC 检查与删除之间的 TOCTOU。旧版单字节 marker 可直接
  读取并映射为本地 owner；滚动升级期间为避免跨 Pod owner 漂移阻塞恢复，旧格式接受
  任意请求 owner 后执行 exact-value CAS 删除。无需离线迁移，解除后再次激活自然写入
  新格式并启用严格 owner 校验。

  三 endpoint 黑盒门禁现要求 Pod A activate 与 Pod B list 返回完全相同且非零的
  `AlarmMember`，Pod C 用虚构 ID 不能解除，再用持久 owner 成功 disarm。真实
  3 PD/3 TiKV、三个共享独立 `a332-alarm-owner` keyspace 的
  `kubebrain:a332-alarm-owner` 首轮 0.296 秒、连续 20 轮 3.493 秒、race 5 轮
  2.115 秒通过；参考 etcd 的完整 13 步 capped/mutation 矩阵串行首轮 0.308 秒、
  连续 10 轮 2.639 秒、race 5 轮 5.548 秒通过。并行运行两组破坏性参考测试会互相
  修改全局 alarm，因此该门禁必须串行。

  backend/server 聚焦 race 各 5 轮、根模块 `go test -p 1 -count=1 ./...`、
  根/compat `go vet ./...`、`git diff --check` 和完整生产 Dockerfile 构建通过；
  镜像 ID
  `sha256:153f9e29078c71c34a5e501b7c254c7371c628257bc0c6850a5090e4795d6ae4`。
  三个测试 Pod 均 Ready、restartCount=0；并发 race 期间 `a332-2` 出现一次 TiKV
  region request `context deadline exceeded` 并自动 refill，之后三个 endpoint
  proposal health 分别为 29.023/29.888/30.113ms。在线
  `kubebrain:a320-object-request-metering` release gate 保持 KubeBrain 3/3、
  PD/TiKV 3+3 Ready，endpoint proposal health 13.667ms。

- **Maintenance A333 explicit alarm owner / uncertain activation reconciliation
  （2026-07-20）**：继续对照
  `/root/etcd/server/etcdserver/apply/backend.go:Alarm` 与
  `/root/etcd/server/etcdserver/api/v3alarm/alarms.go:Activate`，发现 raw
  `Alarm(ACTIVATE, NOSPACE, MemberID=X)` 在 etcd 中持久保存并返回 X，KubeBrain
  此前却始终改写为 serving Pod 的本地 ID。现 RPC 将请求 member 传入 backend；
  非零 owner 原样编码到持久 alarm，零值仍回退 backend identity 派生的稳定 ID，
  保持既有 `etcdctl`/自动配额路径不返回无效 owner。单 keyspace 仍只保存一条
  tenant-global NOSPACE，重复或并发激活采用 Put-if-absent，全部 caller 返回首个
  已持久 owner，不伪造上游任意 member 各自持有多条 alarm 的拓扑。

  同时关闭激活 CAS “提交成功但响应丢失”的不确定结果：仅当 storage 返回
  `ErrUncertainResult` 时，使用独立 5 秒预算线性化回读；短暂
  `ErrUnavailable` 每 50ms 重试，读到任一持久 alarm 即按幂等成功，确认缺失则保留
  原 uncertain error，永久读取错误与原错误聚合返回。确定性测试分别注入 commit-then-
  uncertain（再叠加两次 transient read failure）和 uncommitted-uncertain，证明前者
  返回请求 owner、后者不能伪报成功；32 路不同 owner 并发激活也只产生一个可读 owner。

  raw gRPC 差分把既有 13 步 capped/mutation 场景的 activate 改为固定
  `MemberID=424242`，修复前会在 KubeBrain 端稳定失败。参考 etcd 与使用独立
  `a333-explicit-owner` keyspace 的真实 TiKV-backed
  `kubebrain:a333-explicit-alarm-owner` 首轮 0.357 秒、连续 20 轮 5.709 秒、
  race 5 轮 2.697 秒通过；测试 Pod Ready、restartCount=0，PD/TiKV 3+3 Ready，
  endpoint proposal health 17.780ms，日志无 panic/fatal/storage error。

  聚焦 backend 20 轮、race 10 轮、server 10 轮，根模块
  `go test -p 1 -count=1 ./...`、根/compat `go vet ./...`、`git diff --check`
  和完整 production Dockerfile 构建通过。首次全量运行因参考 etcd 占用测试固定的
  12379/12380 而污染 `pkg/endpoint`，该结果废弃；停止参考服务后 endpoint 单包及
  全量均通过。实现提交 `c872ec81de08155674ffe482427e2b2282f61979`，镜像 ID
  `sha256:c17808cb336e3106811559f96dcb5847d0b941a840a7fdf80dbbcb8e71f19fc4`，
  OCI revision 与实现提交一致，运行用户为 `65532:65532`。

- **Maintenance A334 uncertain alarm disarm reconciliation（2026-07-20）**：
  A333 只关闭 activate 的不确定提交；审计 `DisarmNoSpace` 发现其 exact-value CAS
  若在 TiKV 已提交删除后丢失响应，仍向 root 返回错误，客户端重试时又只得到成功
  空列表。对照 `/root/etcd/server/etcdserver/apply/backend.go:Alarm` 与
  `/root/etcd/server/etcdserver/api/v3alarm/alarms.go:Deactivate` 的 Raft apply
  幂等状态变更，本轮为解除路径增加同样的独立 5 秒线性化回读，不把 caller
  cancellation 传播到提交结果判定。

  回读以 CAS 前保存的精确 alarm 字节为判据：key 缺失证明旧 alarm 已删除，返回一次
  removed；值不同且新格式合法，证明旧 alarm 已删除后又被另一 owner 激活，仍返回
  removed 但保持 `quota.nospace=1`；精确旧值仍在时无法区分未提交与同 owner ABA，
  保守保留原 `ErrUncertainResult`。短暂 `ErrUnavailable` 每 50ms 重试；畸形替代值、
  永久读取错误或预算超时与原提交错误聚合，不能伪报成功。

  backend 确定性故障注入覆盖 commit-then-uncertain 后两次 transient read failure、
  uncommitted-uncertain，以及在回读阻塞窗口中由不同 owner 重激活三种状态。聚焦
  Arm/Disarm 20 轮、race 10 轮和 server Alarm 10 轮通过；一次并行 backend/server
  全包运行中 backend 高负载用例失败且输出被截断，单独串行 backend 42.915 秒通过，
  该并行结果不计入门禁。

  参考 etcd 与使用独立 `a334-uncertain-disarm` keyspace 的真实 TiKV-backed
  `kubebrain:a334-uncertain-alarm-disarm` 正常 mutation/capped 状态机首轮
  0.340 秒、连续 20 轮 5.550 秒、race 5 轮 2.641 秒通过；uncertain commit 分支
  由可控 batch 故障注入证明，不以不可控网络断连猜测提交结果。测试 Pod Ready、
  restartCount=0，PD/TiKV 3+3 Ready，最终 endpoint proposal health 27.702ms，
  日志无 panic/fatal/storage error。

  根模块 `go test -p 1 -count=1 ./...`、根/compat `go vet ./...`、
  `git diff --check` 和完整 production Dockerfile 构建通过。实现提交
  `a2757e961b31202a15a60fbdc9774d4d1e3f4eca`，镜像 ID
  `sha256:5f286aab4a660af32506abe680be582dab266edd115b055b188e8937cf7bbe19`，
  OCI revision 与实现提交一致，运行用户为 `65532:65532`。

- **Maintenance A335 quota startup reconciliation（2026-07-20）**：继续审计
  A328 的 readiness 前配额初始化。首次写入 `quota/usage` 若 TiKV 已提交但响应丢失，
  旧实现会直接启动失败；存量 usage 超过新 quota 时又通过重复的 raw batch 激活
  NOSPACE，未继承 A333 的不确定提交回读。对照
  `/root/etcd/server/etcdserver/api/v3rpc/quota.go` 的 quota alarmer 和
  `/root/etcd/server/etcdserver/server.go` 的 alarm 恢复语义，本轮让初始化在独占
  logical write barrier 内复用正常 CAS 路径，并把首次 usage 创建纳入独立 5 秒
  线性化回读：读到合法 8 字节 usage 即按已提交继续，确认 key 缺失则保留原
  `ErrUncertainResult`，短暂 `ErrUnavailable` 每 50ms 重试，永久读取/解码/超时错误
  与原提交错误聚合。启动超额改为调用统一 `activateNoSpace`，继承显式首 owner 和
  uncertain activation reconciliation，且不会在持有 barrier 时重入死锁。

  确定性 batch 故障注入覆盖 usage commit-then-uncertain 后两次 transient read
  failure、usage uncommitted-uncertain，以及存量超额时 alarm
  commit-then-uncertain 后两次 transient read failure；这些分支由存储层可控注入
  证明，不以不可控网络断连猜测提交结果。quota 初始化 20 轮、race 10 轮，以及与
  range transaction barrier 组合的 20 轮和 race 10 轮均通过；聚焦 quota suite、
  backend vet、根/compat vet 和根模块 `go test -p 1 -count=1 ./...` 通过。

  真实 3 PD/3 TiKV 使用独立 `a335-quota-startup` keyspace 做两阶段重启：第一阶段
  无 quota 写入 114 字节存量，第二阶段以相同 identity/keyspace 和
  `--quota-backend-bytes=32` 启动。新 Pod UID
  `ebb17321-0670-4e68-a5d0-f33441fbb50d` Ready、restartCount=0，`/ready` 和
  `/readyz?verbose` 全部通过；`alarm list` 返回 member 293549777 的 NOSPACE，
  Status 返回 `DbSize=DbSizeInUse=114`、`DbSizeQuota=32`，存量 key 可读，新 Put
  返回标准 `ResourceExhausted: etcdserver: mvcc: database space exceeded`。
  active NOSPACE 下 `etcdctl endpoint health` 按 etcd 契约报告 unhealthy，但这不
  表示 serving/readiness 失败。日志无 quota 初始化错误、panic/fatal/storage error。

  实现提交 `67995f46a17076f7849ae675ffa8e3b6aa96052d`；production 镜像
  `kubebrain:a335-quota-startup-reconcile` 的 ID 为
  `sha256:3cadba2ecc14dafc0db99dd2b9f5a1527edcb17d5023500fed751d8401d5188f`，
  OCI revision 与实现提交一致，运行用户为 `65532:65532`。

- **Maintenance A336 quota re-enable usage rebuild（2026-07-20）**：A335 仍会在
  找到合法 `quota/usage` 时快速恢复，但该元数据只在 quota 启用期间随用户 mutation
  更新。可复现序列为：启用 quota 写入并产生 usage，禁用 quota 后增加数据，再重新
  启用；旧实现直接信任禁用窗口前的 usage，低估存量并可能放行超额写。

  本轮新增 tenant 内部 `quota/tracking` clean/dirty 状态。禁用 quota 的 leader 必须
  在 serving 前持久写 dirty；启用时只有 clean 且 usage 合法才走快速恢复，否则在
  logical write barrier 内扫描指定 revision 的全部 live key，并用单个 TiKV batch
  CAS 原子更新精确 usage 与 clean marker。旧版本没有 marker、未知 marker、dirty
  marker 或 usage 缺失均 fail closed 并重建；`QuotaStatus` 和 mutation 热路径也检查
  clean marker，不能静默消费陈旧值。CAS 冲突由启动重试重新扫描，不采用可能仍基于
  陈旧 base 的冲突值；不确定提交继续按 A335 同时回读 clean marker 与合法 usage。

  backend 回归覆盖 enabled(2 B) -> disabled write(usage 元数据仍为 2 B) ->
  re-enabled 的 4 B 精确重建和 3 B quota 自动 NOSPACE，以及旧版本无 marker、伪造
  usage=1 时按真实 `legacy` key+value 重建。quota 聚焦套件、聚焦 race 10 轮、
  backend 全包、server/etcd 全包、根模块 `go test -p 1 -count=1 ./...`、根模块与
  `hack/etcd-client-compat` 的 `go vet ./...`、`git diff --check` 均通过。

  真实 3 PD/3 TiKV 使用独立 `a336-quota-reenable` keyspace 做三阶段 Pod 重建：
  quota=256 时写入 23 B 并建立 clean usage；禁用 quota 后新增 54 B，Status 按禁用
  契约返回 sentinel；随后 quota=32 启动，在 readiness 前重建为精确 77 B 并激活
  member 1553413814 的 NOSPACE。最终 Pod UID
  `7c631b6e-b664-49fa-a9e7-35f1ff372cc1` Ready、restartCount=0，
  `/readyz?verbose` 全部通过，两条跨阶段存量均可读，新 Put 返回标准
  `ResourceExhausted: etcdserver: mvcc: database space exceeded`，日志无 quota
  初始化错误、panic/fatal/storage error，PD/TiKV 3+3 Ready。

  实现提交 `756730f5757b2cd42ce8076de4bee0feb2909e20`；production 镜像
  `kubebrain:a336-quota-reenable-rebuild` 的 ID 为
  `sha256:ce206ad48336beaf6c95bfcf82c7b39b402f93f7845da26e77c92a3c81574a97`，
  OCI revision 与实现提交一致，运行用户为 `65532:65532`。同一 keyspace 的所有
  serving 副本必须使用一致 quota 配置；滚动混配时 dirty marker 会让启用副本
  fail closed，不能把该保护当作长期混合配置支持。

- **Maintenance A337 over-quota alarm disarm（2026-07-20）**：继续对照
  `/root/etcd/server/etcdserver/apply/backend.go:Alarm` 与
  `/root/etcd/server/etcdserver/api/v3alarm/alarms.go:Deactivate`。upstream
  DEACTIVATE 只按 member/type 删除持久 alarm，不读取 backend quota；KubeBrain
  此前却在 `usage >= quota` 时返回 `ResourceExhausted`，使
  `etcdctl alarm disarm` 多出非标准容量前置条件。

  参考 etcd 以 `--quota-backend-bytes=1` 启动，Status 为
  `DbSize=28672, DbSizeQuota=1`：Put 激活 NOSPACE 后，仍超额时 `alarm disarm`
  成功且 list 为空；下一次 Put 返回标准 NOSPACE 并重新激活同 member alarm。
  KubeBrain 现同样允许 owner 在 exact/over-quota 状态解除 alarm，同时收紧无 alarm
  写入预检：当前 usage 已达到 quota 时，任一含 Put 的请求（包括 shrinking/no-op Put
  和混合 Txn）都会在分配 revision 前重新激活 NOSPACE 并拒绝；delete-only 仍可降容。
  从低于 quota 增长到恰好等于 quota 的首次 Put 继续成功。

  backend 与 RPC 回归固定 exact-limit disarm -> shrinking Put re-arm ->
  delete-only -> second disarm -> recovered Put 状态机；聚焦 20 轮、race 10 轮、
  backend/server 全包、根模块 `go test -p 1 -count=1 ./...`、根模块与
  `hack/etcd-client-compat` 的 `go vet ./...`、`git diff --check` 均通过。

  真实 3 PD/3 TiKV 使用独立 `a337-overquota-disarm` keyspace 和 quota=16：
  7 B key + 9 B value 首次写到精确 16 B；下一 Put 激活 member 3508521812 的
  NOSPACE；16/16 B 时 disarm 成功且 list 清空；shrinking Put 随即返回
  `ResourceExhausted` 并重新激活；delete-only 后再次 disarm，`/ok=z` 成功且 Status
  为 4/16 B。Pod UID `37f24614-33b0-499d-ac10-d210bb75730b` Ready、
  restartCount=0，`/readyz?verbose` 全部通过，最终 endpoint proposal health
  18.004ms，日志无 quota 初始化错误、panic/fatal/storage error，PD/TiKV 3+3 Ready。

  实现提交 `1a9fbf54cdab5a729d3c7cbced5635673ccbceda`；production 镜像
  `kubebrain:a337-overquota-disarm` 的 ID 为
  `sha256:7c7963e0dd019e9b568f8f0894d5f737062c7996e28bdeb6b93f76504b998a05`，
  OCI revision 与实现提交一致，运行用户为 `65532:65532`。

- **Maintenance A338 concurrent alarm disarm idempotency（2026-07-20）**：
  对照 `/root/etcd/server/etcdserver/api/v3alarm/alarms.go:Deactivate`，upstream
  在 `AlarmStore.mu` 下串行查询并删除；并发重复解除只有一个请求返回
  `AlarmMember`，其余成功空列表。KubeBrain 的 `DisarmNoSpace` 为避免错误 owner
  删除新 alarm，会先读 Status、再读精确 alarm 字节、最后 CAS；若另一请求恰在两次
  读取之间完成删除，后一个请求此前把 `ErrKeyNotFound` 直接暴露为 RPC 错误，破坏
  幂等契约。

  现第二次精确读取发现 key 已缺失时，以该读取作为“另一解除已先线性化”的点，更新
  `quota.nospace=0` 并返回 `(removed=false, nil)`；RPC 因而返回成功空列表。CAS
  conflict、不同 owner replacement、畸形值、永久读取错误和 uncertain commit 仍沿用
  A333/A334 的保守分支，不被错误归类为成功删除。

  确定性 storage wrapper 暂停第一个请求的第二次 alarm Get，让另一个请求完成删除后
  再恢复，修复前稳定得到 `ErrKeyNotFound`；修复后 100 轮和 race 20 轮均为一个
  removed、一个成功 empty，完整 Arm/Disarm race 10 轮、backend/server 全包、
  根模块 `go test -p 1 -count=1 ./...`、根模块与
  `hack/etcd-client-compat` 的 `go vet ./...`、`git diff --check` 均通过。

  真实 3 PD/3 TiKV 使用独立 `a338-concurrent-disarm` keyspace、quota=128：
  每轮以 200 B value 的被拒 Put 激活 member 1881591022 的 NOSPACE，再由 64 个独立
  `etcdctl alarm disarm` 并发解除，连续 20 轮共 1280 请求零失败；每轮恰好一个响应
  返回 removed alarm，累计 20 个 removed、1260 个成功 empty，且每轮最终 list 为空。
  Pod UID `01a72a34-8059-4a47-af27-419074eb5d94` Ready、restartCount=0，
  `/readyz?verbose` 全部通过，最终 endpoint proposal health 21.861ms，日志无 quota
  初始化错误、panic/fatal/storage error，PD/TiKV 3+3 Ready。

  实现提交 `2d0b1566bb2d72963524c1a2f2369b5b612863f9`；production 镜像
  `kubebrain:a338-concurrent-disarm` 的 ID 为
  `sha256:abae028e361cd69180a10a5ea7479725ad22402261e152d24f37ba045f557a89`，
  OCI revision 与实现提交一致，运行用户为 `65532:65532`。

- **Maintenance A339 alarm activation owner linearization（2026-07-20）**：
  A338 后审计对称的 ACTIVATE/DEACTIVATE 交错。对照
  `/root/etcd/server/etcdserver/api/v3alarm/alarms.go:Activate`，upstream 在锁内
  持久化后直接返回该 `AlarmMember`；KubeBrain 此前先执行 Put-if-absent，再额外
  `NoSpaceAlarm` 回读 owner。若新 alarm 已提交、并发 disarm 在回读前删除，
  ACTIVATE 会成功却返回 `MemberID=0`；若 Put-if-absent 因既有 owner 冲突，而该
  owner 又在冲突回读前被解除，也会出现相同无效响应。

  激活 helper 现直接返回线性化时确定的 owner：新建提交成功立即返回请求 owner，
  即使之后的 DEACTIVATE 已成为最终状态；CAS conflict 时回读仍存在的合法 alarm 并
  返回首 owner，若 key 已消失则重试 Put-if-absent，直到本次 owner 成功持久化或读到
  另一个持久 owner。uncertain commit 仍用独立预算回读；确认缺失时保留原 uncertain
  error，不能因重试生成无法判定的双重提交结果。

  两个确定性 storage wrapper 分别在 alarm create 已提交但 Commit 尚未返回时完成
  并发 disarm，以及在 CAS conflict 后 owner 回读前完成 disarm。修复前两者稳定返回
  owner 0；修复后各 100 轮、race 20 轮通过，前者返回已提交 owner 且最终状态为
  disarmed，后者重试并持久化请求 owner。完整 Arm/Disarm race 10 轮、
  backend/server 全包、根模块 `go test -p 1 -count=1 ./...`、根模块与
  `hack/etcd-client-compat` 的 `go vet ./...`、`git diff --check` 均通过。

  真实 3 PD/3 TiKV 使用独立 `a339-activation-owner` keyspace、quota=128：
  每轮先持久 seed owner，再让 32 个不同显式 owner ACTIVATE 与 32 个 seed owner
  DEACTIVATE 同时竞争，连续 20 轮。640 个激活响应全部恰好包含一个非零 owner，
  640 个解除请求零 RPC 错误，最终 alarm 可完整清理。Pod UID
  `eed59df7-3d13-438b-bec7-57c3b88de966` Ready、restartCount=0，
  `/readyz?verbose` 全部通过，最终 endpoint proposal health 20.573ms，日志无 quota
  初始化错误、panic/fatal/storage error，PD/TiKV 3+3 Ready。

  实现提交 `e2be437cddce808332c000ca3a317765a9f052c8`；production 镜像
  `kubebrain:a339-activation-owner-linearization` 的 ID 为
  `sha256:9394b0dec6036f04dbaf6150c07d52c5bda9126a5b47d6e58b5be053949cc484`，
  OCI revision 与实现提交一致，运行用户为 `65532:65532`。

- **Production A340 bounded Docker build context（2026-07-20）**：连续 production
  镜像构建审计发现 daemon context 为 448.3 MB，而 689 个 tracked file 合计仅约
  7.8 MB。根因是 `.gitignore` 已排除但 `.dockerignore` 漏掉的本地 `.dev`：四份
  kube-apiserver 二进制、版本矩阵和 smoke 工件共约 422 MB；它们不参与 Dockerfile
  的任何 COPY/编译，却放大每次上传、缓存失效和本地敏感测试工件暴露面。

  `.dockerignore` 现同步排除 `.dev`、`.claude`、IDE 配置、`output` 及根级
  kube/loadgen/smoke 二进制，并保留原 `.git`、`bin`、coverage 等规则。新增
  `build/dockerignore_test.go` 固定高风险排除项，同时断言 `build/cmd/hack/pkg` 和
  `go.mod/go.sum` 不可被根级规则误排。聚焦 build test 20 轮、build/production
  test+vet、根模块 `go test -p 1 -count=1 ./...`、根模块与
  `hack/etcd-client-compat` 的 `go vet ./...`、`git diff --check` 均通过。

  使用提交 metadata 的完整 TiKV production Dockerfile 构建 context 从 448.3 MB
  降至 7.319 MB，约减少 98.4%，全部 47 个 build stage 成功。镜像内
  `kube-brain version` 为 3.7.0/TiKV、Git SHA
  `ccc3890190f271a23e7b4066bd24ae4c3ff217e3`、Go 1.26.5、UTC build time，与 OCI
  label 完全一致。镜像 ID
  `sha256:f2b8199801a472eaf98b43e1b9a081fa79291e4da93dfac66041186735efb0e2`，
  运行用户为 `65532:65532`。

  `kubebrain:a340-docker-context` 在独立 `a340-docker-context` keyspace、真实
  3 PD/3 TiKV 上 Ready；Put/Get 返回 `/a340/context=compact`，四项 readyz 全部
  通过，endpoint proposal health 97.755ms。Pod UID
  `f68b124b-1fcc-44ac-b8dc-4dc1da01dd5e`、restartCount=0，日志无
  initialization failure/panic/fatal/storage error，PD/TiKV 3+3 Ready。

- **Production A341 nested module dependency cache（2026-07-20）**：A340 将 Docker
  context 收敛后，继续审计原 47-step production Dockerfile，发现根模块已在
  `COPY . .` 前缓存 `go mod download`，但独立
  `hack/backup/objectstore/go.mod` 仍随全部源码复制；任意普通源码变更都会使其 AWS
  SDK 等依赖在最终编译步骤重新解析和下载。

  Dockerfile 现先复制 objectstore 的 `go.mod/go.sum`，在独立层执行
  `cd hack/backup/objectstore && go mod download`，再复制完整源码。新增严格顺序测试
  固定“根模块依赖、objectstore 依赖、完整源码”边界，聚焦测试连续 20 轮通过。
  初次 A341 构建 context 为 7.322 MB，新增 objectstore 下载层成功完成；随后加入一个
  未被 ignore 的临时源码探针，第二次完整构建 context 为 7.323 MB：根模块
  `go mod download`、objectstore module COPY 及其 `go mod download` 均明确显示
  `Using cache`，`COPY . .` 与生产编译则重新执行，编译日志没有 AWS module 下载。
  探针已在验证后删除，两次构建产物均为同一镜像 ID
  `sha256:1decc8ba6cabf328673fc11c0801a6a4036d9ec80504f8727104ae886c807a81`。

  镜像 `kubebrain:a341-objectstore-module-cache` 内 `kube-brain version` 为
  3.7.0/TiKV、Git SHA `28e064582c5a1d71152a5c75f3ed6ecac444951d`、Go 1.26.5、
  UTC build time，OCI labels 与之完全一致，运行用户为 `65532:65532`。根模块
  `go test -p 1 -count=1 ./...`、根模块与 `hack/etcd-client-compat` 的
  `go vet ./...`、`git diff --check` 均通过。

  该镜像在独立 `a341-objectstore-cache` keyspace、真实 3 PD/3 TiKV 上 Ready；
  Put/Get 返回 `a341/production-ready=verified`，四项 readyz 全部通过，endpoint
  proposal health 94.94675ms。Pod UID
  `61f63029-912c-4313-9537-e1f561a65a8f`、restartCount=0，日志无
  panic/fatal/segmentation/data race，PD/TiKV 3+3 Ready。

- **Compatibility A342 maintenance auth differential completion（2026-07-20）**：
  对照 `/root/etcd/server/etcdserver/api/v3rpc/maintenance.go` 的
  `authMaintenanceServer`，复核维护 RPC 的鉴权优先级。现有双端 Auth 差分仅覆盖
  Status、Alarm 和 HashKV；Defragment、流式 Snapshot、raw Hash、MoveLeader、
  Downgrade 虽有进程内测试，但没有证明真实 gRPC token 提取、stream context 和
  平台替代错误之前的鉴权顺序。

  `TestAuthDifferentialAgainstEtcd` 现同时记录上述五类 RPC 的匿名和普通非 root
  结果，并要求参考 etcd 与 KubeBrain 的 canonical gRPC code/message 完全相等：
  匿名请求均为 `etcdserver: user name is empty`，非 root 请求均为
  `etcdserver: permission denied`，不能先泄露 Snapshot/MoveLeader/Downgrade 的
  平台边界。SnapshotWithVersion 与 raw generated client 会保留 canonical
  code/message、但其包装 error 不保证 `errors.Is`；`authError` 因此同时按 upstream
  精确 code/message 识别 `ErrGRPCUserEmpty`，没有放宽结构化比较。

  全新 reference etcd 与独立 `a342-maintenance-auth-race` TiKV keyspace 的完整 Auth
  生命周期及新增矩阵通过，race 运行 10.985 秒通过。完整 compat suite 使用显式
  reference/KubeBrain endpoint、单线程运行，376.397 秒通过。期间还发现
  `TestPlatformManagedOperationsReturnActionableErrors` 仍把 NOSPACE Alarm mutation
  当作 Unimplemented；A330-A339 已实现该兼容能力，当前矩阵也已升级，因此删除这个
  与产品状态冲突的陈旧断言。修正后的平台边界测试和 lease generation
  linearizability baseline 各连续 10 轮通过。

  根模块 `go test -p 1 -count=1 ./...`、根模块与 compat 的 `go vet ./...`、
  `git diff --check` 均通过。A342 只增强兼容测试，不改变服务二进制，因此继续使用
  已提交源码构建的 `kubebrain:a341-objectstore-module-cache`。该镜像在独立 keyspace
  上 Pod UID `0162a49a-2933-421c-a760-cc144f36046f` Ready、restartCount=0，
  `/ready` 和四项 readyz 全部通过，日志无 panic/fatal/segmentation/data race，
  PD/TiKV 3+3 Ready。

- **Compact A343 safe physical-GC carve-outs（2026-07-20）**：审计 TiKV 平台特有的
  `--skip-key-prefix`（非 upstream etcd wire protocol）时发现，启动校验错误地要求用户
  前缀位于内部协调 `--prefix` 下。A62 已把内部 prefix 与用户数据解耦，该限制使真实
  `/registry/...` carve-out 无法启动。重复或嵌套前缀还会生成重叠排序边界，使一次扫描
  重新进入本应排除的区间。

  启动配置现允许独立于内部 prefix/keyspace 的用户前缀，同时拒绝空值、尾随 `/`、
  重复和嵌套重叠；错误在连接存储前 fail closed。后端对直接构造的配置另做排序、去重
  和嵌套折叠。结果级测试分别写入 included/excluded key 的四个版本并强制完整 physical
  scan，included 只剩当前版本，excluded 保留全部四个版本；逻辑 compact watermark
  不受 carve-out 影响。实现提交
  `057210e2af9c5eb8cb468073ae775ca1f6d13b45`，结果测试提交 `8871b6c`；
  option/backend 聚焦测试 20 轮、race 3 轮通过。

  包含该修复的最终镜像 `kubebrain:a344-metadata-cache` 在独立
  `a344-safe-skip-prefix` keyspace、真实 3 PD/3 TiKV 上以
  `--skip-key-prefix=/registry/a344-excluded` Ready。included/excluded key 更新后
  physical compact 到 revision `467812086451011589`，两者最新值均为 `v2`；
  `/ready` 与四项 readyz 通过，endpoint proposal health 26.279411ms。Pod UID
  `20cc4daf-a300-4334-860d-415e67c392b9`、restartCount=0，日志无
  panic/fatal/segmentation/data race/storage error。独立容器传入 `/registry` 与
  `/registry/pods` 在存储连接前退出 1，并报告 overlap。

- **Production A344 release metadata cache isolation（2026-07-20）**：A343 production
  构建暴露 version/SHA/date `ARG` 位于依赖层之前，导致每个新提交都重新执行根模块和
  objectstore 的 `go mod download`；runtime OCI `LABEL` 位于 `apk add` 之前，也使
  revision 变化冲掉运行时包安装及后续复制缓存。

  Dockerfile 现把 build-stage metadata ARG 移到 `COPY . .` 后、编译前，把 runtime
  ARG/LABEL 移到包安装和所有 COPY 后；严格顺序测试连续 20 轮通过。由实现提交
  `08fd11bb7fe81d3655166e95c1f779b27fe11630` 构建的
  `kubebrain:a344-metadata-cache` 镜像 ID 为
  `sha256:95720577f2430389882c7d723b3984c62e0338cddab857a246193bc058e6d3aa`，
  OCI revision 与二进制 version 一致，版本 3.7.0/TiKV、Go 1.26.5、运行用户
  `65532:65532`。仅把 SHA 改为测试值的 probe 镜像 ID 为
  `sha256:242bab03e96583a10a942034427dad0321e12fe0d86000c3927cf07995da0ea3`：
  两个 module COPY/download、完整源码 COPY 和 runtime `apk add` 均命中缓存，仅编译
  与 metadata 层重建；build context 为 7.333 MB。

  根模块完整测试、根/compat `go vet ./...`、`git diff --check` 均通过；真实 endpoint
  上 Compact/Compaction/PhysicalCompaction 定向 compat 回归 6.477 秒通过。A343 的
  `8871b6c` 只新增结果测试，晚于运行镜像 revision，不改变被验证的数据面二进制。

- **Production A345 immutable base-image inputs（2026-07-20）**：production artifact
  已要求完整源码 SHA、OCI labels 和最终 image digest，但 Dockerfile 的
  `golang:1.26-bookworm`、`alpine:3.23` 仍是可变 tag；同一 KubeBrain commit 在 tag
  漂移后可能静默使用不同编译器、系统库或根文件系统，现有 provenance 无法证明这些
  外部构建输入。

  两个 stage 现分别固定到 OCI index digest
  `sha256:1ecb7edf62a0408027bd5729dfd6b1b8766e578e8df93995b225dfd0944eb651`
  和 `sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40`，
  同时保留可读 tag。远端 manifest 校验确认两者均为 OCI image index，并包含
  `linux/amd64` 子 manifest。新增构建门禁枚举每个 `FROM`，要求
  `tag@sha256:<64 hex>`，避免新增 stage 或后续重构退回 tag-only；聚焦测试 20 轮、
  race 10 轮及根/compat vet 通过。

  从实现提交 `e9319c275cfea82a87b96f45b321d9d5d68e5cc7` 的 `git archive` 完整构建
  `kubebrain:a345-pinned-base-images`，context 7.341 MB，49 个 stage 全部成功；
  image ID
  `sha256:b58db02d6c1c5fba34f542004a6d19eca9fe11850d9cc5055a4527e58c0f07c6`，
  OCI revision 与二进制 version 均为该完整提交，版本 3.7.0/TiKV、Go 1.26.5、
  linux/amd64、运行用户 `65532:65532`。

  该镜像在独立 `a345-pinned-base-images` keyspace、真实 3 PD/3 TiKV 上 Ready。
  Put/Get/Delete `/a345/production-ready` 成功，endpoint proposal health
  29.611449ms，`/ready` 与四项 readyz 全部通过。Pod UID
  `0e363602-7041-4684-a341-9a86a25833d3`、restartCount=0，日志无
  initialization failure/panic/fatal/segmentation/data race/storage error；PD/TiKV
  3+3 Ready。

- **Production A346 pinned runtime package inputs（2026-07-20）**：A345 固定两个
  base image OCI index 后继续审计构建输入，发现 runtime `apk add` 的八个显式包仍未
  指定版本。Alpine v3.23 仓库更新会让同一源码和同一 base digest 静默获得不同
  `kubectl`、`etcdctl`、OpenSSL 或 CA bundle，最终 image digest 虽变化，但源码
  provenance 不能解释差异。

  Dockerfile 现固定 `bash=5.3.3-r1`、`ca-certificates=20260611-r0`、
  `coreutils=9.8-r1`、`curl=8.20.0-r0`、`etcd-ctl=3.6.10-r1`、
  `jq=1.8.1-r0`、`kubectl=1.34.2-r6` 和 `openssl=3.5.7-r0`。构建测试提取
  `apk add` block，要求包集合与精确版本完全相等，新增或升级必须显式评审；固定
  Alpine image 上的独立安装探针和最终镜像 `apk list --installed` 均逐项匹配。
  聚焦测试 20 轮、race 10 轮及根/compat vet 通过。该门禁固定顶层包；长期离线
  字节级重建仍需受控 APK repository snapshot 或保留最终 digest 镜像。

  从实现提交 `9b38f0690814021c8412c130e1f759002dd2a28c` 的 `git archive` 构建
  `kubebrain:a346-pinned-runtime-packages`，context 7.345 MB，两个 module
  COPY/download 均命中缓存，49 个 stage 全部成功。image ID
  `sha256:ad1ec8f5a0dd350428561934d48338a13d802cc1df64f0edc8c35c169efb81c5`，
  OCI revision 与二进制 version 均匹配完整提交，版本 3.7.0/TiKV、Go 1.26.5、
  linux/amd64、运行用户 `65532:65532`。

  该镜像在独立 `a346-pinned-runtime-packages` keyspace、真实 3 PD/3 TiKV 上
  Ready。Put/Get/Delete `/a346/production-ready` 成功，endpoint proposal health
  24.116694ms，`/ready` 与四项 readyz 全部通过。Pod UID
  `40ce1d07-a33f-44b7-b88f-90a733c75e88`、restartCount=0，日志无
  initialization failure/panic/fatal/segmentation/data race/storage error；PD/TiKV
  3+3 Ready。

- **Production A347 kubectl/server skew alignment（2026-07-20）**：A346 固定软件包后
  发现 production image 内 Alpine `kubectl v1.34.2` 与仓库已声明、系统验证的
  Kubernetes v1.35/v1.36 server 窗口不一致。Kubernetes 只保证 kubectl 与 apiserver
  相差不超过一个 minor；1.34 client 访问 1.36 server 超出保证，而该二进制被 restore
  cutover、certificate rotation、destroy、release gate 等生产脚本广泛调用。

  Dockerfile 现从官方 `dl.k8s.io` 获取 v1.36.2，并对 amd64
  `1e9045ec32bea85da43de85f0065358529ea7c7a152eca78154fba5b58c27d82`、arm64
  `c957eb8c4bea27a3bb35b269edd9082e27f027f7b76b20b5bf4afebc726c6d3e`
  分别执行 SHA-256 校验；下载只允许 HTTPS/TLS 1.2+，未知 `TARGETARCH` 在
  `COPY . .` 和源码编译前 fail closed。Alpine 1.34.2 包已移除，最终 runtime 只复制
  校验后的官方二进制。构建门禁固定版本、双架构 digest、URL、校验和 COPY 路径；
  聚焦测试 20 轮、race 10 轮通过。arm64 本轮只验证官方 checksum 供应链，仍需 arm64
  runner 做真实构建与运行。

  从实现提交 `c23f3b6f29715f38f453b7b605c80ba190f20cdb` 的 `git archive` 构建
  `kubebrain:a347-kubectl-skew`，context 7.348 MB，两个 module download 命中缓存，
  `/src/bin/kubectl: OK` 后 53 个 stage 全部成功；s390x 负向构建在源码编译前明确
  报 `unsupported TARGETARCH=s390x`。image ID
  `sha256:f0a21158b6d2599e62aee70aa2421b4effd108013955e1bdced66dcc8be84882`，
  OCI revision 与二进制 version 匹配，版本 3.7.0/TiKV、Go 1.26.5、linux/amd64、
  运行用户 `65532:65532`。

  最终非 root 镜像内 `kubectl version` 为 client v1.36.2，并成功连接 kind
  Kubernetes v1.36.1 API、读取 `kubebrain-dev` namespace。相同镜像在独立
  `a347-kubectl-skew` TiKV keyspace 上 Ready；Put/Get/Delete
  `/a347/production-ready` 成功，endpoint proposal health 25.749785ms，
  `/ready` 与四项 readyz 全部通过。Pod UID
  `1aca73cc-350b-4a1c-90c9-a9e3b0acba14`、restartCount=0，日志无
  initialization failure/panic/fatal/segmentation/data race/storage error；PD/TiKV
  3+3 Ready。

- **Production A348 target-architecture binary coherence（2026-07-20）**：A347 按
  `TARGETARCH` 选择官方 kubectl 后继续审计 multi-arch 路径，发现全部 KubeBrain 和
  运维 Go 二进制仍继承 builder 主机架构。以 `TARGETARCH=arm64` 发布时会得到 arm64
  kubectl 与 amd64 数据面混装的不可启动镜像，且 `kube-brain version` 错报
  `linux/amd64`。

  build stage 现设置 `GOOS=linux GOARCH=${TARGETARCH}`，覆盖 `build-tikv.sh`、
  `build-badger.sh` 以及 Dockerfile 内全部 production/backup/objectstore `go build`。
  顺序门禁要求该环境位于源码 COPY 和编译前；`build-base.sh` 实际 metadata 测试以
  `GOARCH=arm64` 运行，固定 ldflags 中 `GoOsArch=linux/arm64`。build 包连续 20 轮和
  race 10 轮通过。

  从实现提交 `890600f6c459e101459531b075d2807a37fcd2ec` 的同一 `git archive`
  分别构建 arm64 build stage 与 amd64 完整 image，context 7.352 MB，两个 module
  download 均命中缓存。arm64 日志显示 kubectl checksum OK、`go_arch=arm64`；
  从 build image 提取的 23 个可执行文件全部为静态 ARM AArch64 ELF，`kube-brain`
  Go metadata 同时记录 `GOARCH=arm64`、`GoOsArch=linux/arm64` 和完整 commit。
  amd64 最终镜像的 23 个可执行文件则全部为 x86-64，version 报
  `linux/amd64`，不存在混合架构。

  amd64 镜像 `kubebrain:a348-cross-arch-coherence` ID 为
  `sha256:f229498aa8f520631123318f2a6040762cb64723ecc40402494e5a1cf87f7782`，
  OCI revision 与二进制 version 匹配，运行用户 `65532:65532`。该镜像在独立
  `a348-cross-arch-coherence` keyspace、真实 3 PD/3 TiKV 上 Ready；Put/Get/Delete
  `/a348/production-ready` 成功，endpoint proposal health 70.349226ms，
  `/ready` 与四项 readyz 全部通过。Pod UID
  `c9dbdc09-f825-4dc4-a1b9-cd58fa3d6280`、restartCount=0，日志无
  initialization failure/panic/fatal/segmentation/data race/storage error；PD/TiKV
  3+3 Ready。当前宿主无 arm64 执行环境，因此 arm64 最终 runtime、Kubernetes API 与
  TiKV/PD smoke 仍须在原生 arm64 CI runner 完成后才能发布 multi-arch manifest。

- **Production A349 reproducible multi-arch release gate（2026-07-20）**：A348 完成
  二进制交叉编译后继续审计实际 GitHub Actions 路径，发现 CI 仍使用 Go 1.22.x，且
  image build 未注入 Dockerfile 已强制要求的 version/full revision/UTC created；
  release workflow 只发布 amd64，也没有 SBOM、provenance 或发布后 OCI index 检查。
  这些问题会使普通 CI image 直接 fail closed，或把未经双架构验证的单架构 image
  标记为生产发布。

  提交 `cb32860` 将 CI 对齐 Go 1.26.5，并为 TiKV/Badger image 统一传入完整 metadata、
  回读 revision/non-root user/kubectl；release 使用 QEMU、Buildx、
  `linux/amd64,linux/arm64`、GHA cache、max provenance 与 SBOM，发布 12 位和完整
  commit SHA tag，并按 digest 校验 raw OCI index 恰好包含两个 Linux runtime
  architecture。提交 `739f5c1` 进一步把 compiler stage 固定到
  `${BUILDPLATFORM}`，由原生 Go toolchain 交叉编译 `${TARGETARCH}`，runtime stage
  继续由 BuildKit 选择目标平台。新增 workflow YAML/契约测试固定 metadata 参数、
  双架构、cache、attestation 和 index 检查顺序；build 包连续 20 轮与 race 10 轮通过，
  未知 s390x 仍在源码编译前 fail closed。后续审计又发现 `latest` 原先与不可变 tag
  同步推送、早于 index verification；提交 `9343714` 将其改为验证 digest 后通过
  `imagetools create` 原子 promotion，并回读确认 `latest` 指向同一 digest。

  从 `739f5c1e5848c885e2e8d19a1454e75b04d3074e` 的 `git archive` 构建 TiKV 与
  Badger amd64 image 均成功。TiKV image
  `sha256:8957f2ab140b70035b87d9555e3f943943fc1c07716eb6eb16907c5f3055a8c0`、
  Badger image
  `sha256:eb5570840ae6aa3403c0e1239a19f81549da420bba2ad464811645abe9fa76bb`；
  两者 OCI revision 与二进制 version 均为完整提交，运行用户 `65532:65532`，
  Go 1.26.5/linux/amd64，kubectl v1.36.2。TiKV image 在独立 `a349` keyspace、
  真实 3 PD/3 TiKV 上 Ready，endpoint proposal health、Put/Get/Delete、
  `/ready` 与 `/readyz` 全部通过，且以只读根文件系统、drop ALL capabilities 运行。

  当前验证宿主没有 docker buildx、registry 写权限或原生 arm64 runtime，因此没有伪造
  已发布 multi-arch index 的结论。GitHub Actions 必须在实际 release ref 上完成双架构
  build/push、SBOM/provenance 和 digest index 检查，并在原生 arm64 runner 补齐
  Kubernetes API、TiKV/PD 与 readiness smoke 后，才可把该 ref 标记为 production
  multi-arch release。

- **Production A350 immutable CI supply chain（2026-07-20）**：A349 保证 image 内容与
  multi-arch index 后继续审计 workflow 自身，发现 checkout/setup-go/upload-artifact
  及四个 Docker Actions 均引用可变 major tag；checkout 默认把 token 留在 git
  credential；integration 无 checksum 下载 kind/kubectl，并直接执行 Helm `main`
  安装脚本，默认 kind node image 也只有可变 tag。手工 image 发布还接受 branch/tag
  `source_ref`，会在解析后移动，且可选择未合并分支进入 packages 写权限 job。

  提交 `0ed2adb` 将 14 个 `uses:` 全部固定到上游 major tag 当前对应的 40 位 commit，
  保留版本注释并对所有 checkout 设置 `persist-credentials: false`。release 只由 main
  push 自动触发；workflow_dispatch 为空时使用触发 commit，显式输入必须是 checkout
  后完全匹配的 40 位 SHA，且是 `origin/main` ancestor，未合并 dbaas HEAD 的负向验证
  正确返回非零，当前 origin/main 完整 SHA 的正向验证通过。

  integration Go 固定为 1.26.5；kind v0.32.0、kubectl v1.36.1、Helm v3.18.4 分别通过
  官方 HTTPS/TLS 1.2 URL 下载并固定 SHA-256，实际重新下载后的三项 `sha256sum -c`
  全部为 OK。默认 kind node 固定为
  `kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5`；
  registry 返回的 OCI index 包含 linux/amd64 与 linux/arm64。新增测试解析全部 workflow，
  拒绝非 40 位 action 引用、checkout credential 遗漏、未校验工具、Helm branch 脚本、
  非 main release 和可变 source ref；核心契约连续 100 轮、build 包 20 轮及 race 10 轮
  通过。GitHub 托管 runner 上的 integration 和有 packages 写权限的 multi-arch
  publish 仍须在合并后真实执行，本地证据不能代替该外部发布门禁。

- **Maintenance A351 active alarm visibility in Status（2026-07-20）**：对照
  `/root/etcd/server/etcdserver/api/v3rpc/maintenance.go` 发现 upstream `Status`
  会在 no-leader error 之后追加所有 active `AlarmMember.String()`。KubeBrain 已在 TiKV
  持久化 NOSPACE、支持跨副本 Alarm GET/disarm 并据此拒绝写入，但 Status 只报告
  no-leader；因此 `etcdctl endpoint status` 和运维控制器可能在写入已被配额封锁时仍把
  endpoint 判断为无 error。

  提交 `1c78d14` 使 Status 复用同一次 `QuotaStatus` 的 active 标志，仅 active 时读取
  持久 alarm owner，按 upstream 字符串原样追加；no-leader 保持在前，owner 元数据读取
  失败则整个 Status 失败，禁止静默假健康。单元测试覆盖 alarm 可见/disarm 消失、
  no-leader+NOSPACE 顺序及 owner 读取失败。提交 `e2b1009`、`4ef1157` 增加单端点
  reference 差分和三副本共享可见性测试，并避免由 compat client 的不同 protobuf
  `String()` 格式重建服务端字符串。

  从 `1c78d14ee32a92f358e05fc615680d246c20f156` 的 `git archive` 构建
  `kubebrain:a351-status-alarm`，image ID
  `sha256:c349332c37e9fdbf5ce5acb87e4b1843b44d07c21e214a907426a7ba7574965e`，
  OCI revision、二进制 SHA、TiKV、Go 1.26.5/linux/amd64 与 `65532:65532` 均匹配。
  对当前 `/root/etcd` reference 和独立 `a351` TiKV keyspace 执行相同 member ID 的
  activate/status/disarm/status，连续 10 轮 `Errors` 完全一致。另在独立 `a351-ha`
  keyspace 启动三 KubeBrain 副本：副本 0 activate 后三个副本 Status 字符串完全一致，
  副本 2 disarm 后三个均为空，连续 10 轮通过。三副本及单副本 Pod 全部 Ready、
  restartCount=0，真实 3 PD/3 TiKV 健康，无 initialization failure/panic/fatal/
  segmentation/data race/storage error。

  聚焦普通 50 轮、race 10 轮、compat 编译、根 `go test -p 1 -count=1 ./...`、根与
  compat `go vet ./...` 均通过；完整 `go test -race ./pkg/server/etcd -count=1`
  255.605 秒通过。CORRUPT alarm 仍由 TiKV/PD 数据完整性与 DBaaS 管理面处理，RPC
  保持明确平台替代；本次只补已实现且会实际阻断写入的 NOSPACE 状态可见性。

- **Maintenance A352 active alarm HTTP health semantics（2026-07-20）**：继续对照
  `/root/etcd/server/etcdserver/api/etcdhttp/health.go`，发现 upstream 传统
  `/health` 在 leader/API 检查前先检查 alarm；active NOSPACE 返回 HTTP 503 和精确
  reason，`exclude=NOSPACE` 可跳过，而 `serializable=true` 只跳过 leader 要求、
  不跳过 alarm。KubeBrain 原先只检查 leader 与 Range，因此配额已阻断增长写入时仍
  返回 HTTP 200。

  提交 `2d37ae2` 使 `/health` fail closed 查询租户 `QuotaStatus`，active 时返回
  `{"health":"false","reason":"ALARM NOSPACE"}`，alarm store 错误返回
  `ALARM ERROR:<error>`；支持重复 `exclude` 参数，并保持 alarm 先于 no-leader。
  健康响应也固定包含空 `reason`。`/ready` 与 `/readyz` 刻意不检查 NOSPACE，与
  upstream readiness 语义一致，使容量故障期间读和恢复操作仍可接流。提交 `bd644b0`
  将告警激活窗口内两个 readiness 端点的 HTTP 200 纳入真实运行差分门禁。

  从 `2d37ae2cde203765e0693f7599329c4e9ddb9925` 的 `git archive` 构建
  `kubebrain:a352-http-health-alarm`，image ID
  `sha256:a8b95082aca753cfe844267e1af13950685991f0bdd2b463d4629d763ab9df4b`，
  OCI revision、Go 1.26.5/linux/amd64 与 `65532:65532` 匹配。在当前
  `/root/etcd` reference 与独立 `a352` TiKV keyspace 上，healthy、active、
  active+serializable、active+exclude、disarmed 的 HTTP 状态码和精确响应体连续
  10 轮完全一致；同一 active 窗口的 `/ready`、`/readyz?verbose` 均为 200。
  KubeBrain Pod UID `6fc12f62-c9a6-4961-a503-d05e5c9b6070`，Ready、
  restartCount=0，以只读根文件系统、non-root、drop ALL 和 RuntimeDefault seccomp
  运行，真实 PD/TiKV 3+3 健康。

  聚焦单元测试 50 轮、目标 race 10 轮、`pkg/server` 20 轮和 live differential
  10 轮通过；完整根测试、`pkg/server` race 及根/compat vet 通过。CORRUPT 仍由
  TiKV/PD 数据完整性与管理面处理，不能通过 `exclude=CORRUPT` 虚构本地 alarm。

- **Maintenance A353 legacy HTTP health response headers（2026-07-20）**：A352 的
  双端门禁只比较状态码和响应体，继续审计
  `/root/etcd/server/etcdserver/api/etcdhttp/health.go` 并以真实 reference HTTP
  响应确认：成功 `/health` 虽返回 JSON 形状的文本，但媒体类型是
  `text/plain; charset=utf-8`，且不带 `X-Content-Type-Options`；失败路径经
  `http.Error` 返回同一媒体类型并带 `nosniff`。KubeBrain 原先复用平台
  `/ready`/`ping` writer，使成功 `/health` 错报 `application/json`。

  提交 `fe6c0c1` 为 legacy `/health` 分离成功 writer，平台 `/ready` 与 `/ping`
  继续保持既有 JSON 契约；单元测试固定成功、NOSPACE 失败和 exclude 恢复三类响应头，
  live differential 对 healthy、active、active+serializable、active+exclude、
  disarmed 的状态码、响应体、`Content-Type` 和 `X-Content-Type-Options` 全量比较。

  从 `fe6c0c1120cf63cce7161fb84dd74e0f1c9ea6f0` 的 `git archive` 构建
  `kubebrain:a353-health-headers`，image ID
  `sha256:55c469b26ecbba8d359ad4d376fcd436119a0852e960973b930e6e7f3ed37875`；
  OCI revision、版本 `0.0.0-a353`、Go 1.26.5/linux/amd64、TiKV 与
  `65532:65532` 均匹配。对当前 `/root/etcd` reference 和独立 `a353` keyspace
  连续 10 轮完整 HTTP 差分通过，告警期 `/ready`、`/readyz?verbose` 仍为 200。
  Pod UID `3d5433e5-617f-4e41-9919-94948840dce4`，Ready、restartCount=0，
  只读根文件系统、non-root、drop ALL、RuntimeDefault seccomp；真实 3 PD/3 TiKV
  全部 Running，日志无 initialization failure/panic/fatal/segmentation/data race/
  storage error。

  聚焦普通 50 轮、race 10 轮、compat 编译、完整根测试、完整 `pkg/server` race
  及根/compat vet 均通过。差分现已覆盖 A352 曾遗漏的 HTTP 表示层，后续改变 legacy
  health writer 会直接触发双端失败。

- **Endpoint A354 client HTTP CORS and Host access control（2026-07-20）**：继续
  对照 `/root/etcd/server/embed/serve.go` 的 access controller、
  `addCORSHeader` 和 OPTIONS 短路，以及
  `/root/etcd/server/etcdserver/server_access_control.go`。upstream 默认
  `--cors=*`、`--host-whitelist=*`，每个 client HTTP 响应带三项 CORS 头；
  OPTIONS 在路由 handler 前直接返回 200。显式 Origin allowlist 只回显匹配项，
  plaintext 未知 Host 返回 421 防止 CVE-2018-5702 DNS rebinding，TLS 请求跳过
  该 Host 检查。KubeBrain 原先没有这层包装，A353 的差分也未比较 CORS。

  提交 `437a07f` 在 client HTTP mux 外增加统一 access controller，新增
  `--cors` 与 `--host-whitelist`，默认均为 `*`；空列表同 upstream 允许全部。
  peer 与 info/metrics 构造器保持不变，避免浏览器访问策略意外扩散到内部或诊断端口。
  单元测试固定默认三项头、OPTIONS 不进入 handler、允许/拒绝 Origin、Host 带端口
  解析、plaintext 421 且拒绝响应不带 CORS，以及 TLS 绕过 Host 防护；CLI 测试固定
  默认值和逗号分隔绑定。live differential 新增 OPTIONS 场景，并对所有 health/alarm
  场景比较 `Access-Control-Allow-Methods/Origin/Headers`。

  从 `437a07fe10ff7f5093a35b7bf7e22e63f6d495e0` 的 `git archive` 构建
  `kubebrain:a354-http-access`，image ID
  `sha256:3b74af5c7dd9bbabaa88c87c8241b3e7c80415af86e9286d051deff792ce83ba`；
  OCI revision、版本 `0.0.0-a354`、Go 1.26.5/linux/amd64、TiKV 与
  `65532:65532` 均匹配。默认配置对当前 `/root/etcd` reference 执行 healthy、
  OPTIONS、active、active+serializable、active+exclude、disarmed 全表示层比较，
  连续 10 轮完全一致。

  同一 exact image 再以 `--cors=https://console.example`、
  `--host-whitelist=allowed.internal` 和独立 `a354-custom` keyspace 运行：允许
  Origin 返回精确三项头和 200，拒绝 Origin 仍完成 health 但不返回 CORS，未知
  plaintext Host 返回 421 且无 CORS，允许 Host 的 OPTIONS 返回空 body 200。
  Pod UID `160a66ae-b03c-41f6-b281-60ec3a6dc96b`，Ready、restartCount=0，
  只读根文件系统、non-root、drop ALL、RuntimeDefault seccomp；真实 3 PD/3 TiKV
  全部 Running，日志无 initialization failure/panic/fatal/segmentation/data race/
  storage error。

  聚焦普通 50 轮、race 10 轮、endpoint/option 全包 20 轮（endpoint 399.589 秒）、
  compat 编译、完整根测试、完整 endpoint/option race 及根/compat vet 均通过。
  `/proxy/health` 经审计确认只属于独立 `etcd grpc-proxy` 进程，普通 etcd server
  不注册；KubeBrain 数据面不伪造该路由。

- **Endpoint A355 etcd v3 JSON/HTTP grpc-gateway（2026-07-20）**：对照
  `/root/etcd/server/embed/config.go` 与 `/root/etcd/server/embed/serve.go`，确认
  upstream 默认启用 generated grpc-gateway，并使用 proto field name、base64 bytes、
  string 形式的 64-bit integer 和忽略未知 JSON 字段的 marshaler。KubeBrain 原 client
  HTTP 入口只有健康检查，所有 `/v3/*` 路由均缺失。

  提交 `bbb39f4` 增加默认开启的 `--enable-grpc-gateway`，注册 KV、Watch、Lease、
  Cluster、Maintenance、Auth generated handlers。gateway 延迟回环连接本机 gRPC，
  因而继续经过现有 admission、auth、metrics、请求大小和并发/速率限制；明文使用
  loopback plaintext，TLS-only 使用同一客户端证书和 CA 建立本机 mTLS。提交
  `faaeb37` 让鉴权层在标准 gRPC `token` 之外接受 gateway 转发的
  `authorization` metadata，并保持 raw token 与 Bearer token 兼容。generated
  JSON 合同测试固定 base64、snake_case、string int64 与 unknown-field discard。

  新增双端 HTTP 差分覆盖 Put/Range/Txn/DeleteRange、Lease 生命周期、MemberList、
  Status、AuthStatus 和 malformed base64；鉴权矩阵覆盖 root/role 建立、AuthEnable、
  anonymous 拒绝、HTTP Authenticate、Authorization token 读及 AuthDisable/清理。
  当前 `/root/etcd` reference 与独立 TiKV keyspace 同跑，两组矩阵各连续 10 轮一致。
  从 `faaeb3753ddb550413630e519cff0c8171bce295` 的 `git archive` 构建
  `kubebrain:a355-json-gateway-auth`，源码镜像 ID
  `sha256:14bf6c378f12592cdcede76f63666b7d2a0eb4deca416d113a221703e633574f`，
  OCI revision、版本 `0.0.0-a355.1`、Go 1.26.5/linux/amd64、TiKV 与
  `65532:65532` 均匹配。

  同一 exact image 在真实 3 PD/3 TiKV 上以 TLS-only + client-cert-auth 启动，
  外部携带客户端证书执行 JSON Put/Range/DeleteRange 成功，证明外部 mTLS 和内部
  gateway mTLS 回环均可用；再以 `--enable-grpc-gateway=false` 启动，`/health`
  保持 200 且 `/v3/kv/range` 精确返回 404。完整根测试、endpoint/etcd/option race
  及根/compat vet 全部通过；聚焦 gateway 普通 50 轮、race 10 轮，auth 普通 50 轮、
  race 10 轮均通过。

  A355 完成时尚未注册 upstream 独立 v3lock/v3election HTTP 服务，该缺口已由 A356
  关闭；官方 client/v3 concurrency recipe 继续通过 KV/Lease/Watch 工作。Watch
  gateway 路由已注册并有 generated handler 合同覆盖，但 HTTP 流式长连接尚未做长时
  故障 soak，发布说明不得把本项扩大为长期流稳定性声明。

- **Concurrency A356 dedicated Lock/Election gRPC and JSON APIs（2026-07-20）**：
  对照 `/root/etcd/server/embed/serve.go`、
  `/root/etcd/server/etcdserver/api/v3lock/lock.go` 和
  `/root/etcd/server/etcdserver/api/v3election/election.go`，确认普通 etcd server
  除 client/v3 recipe 外还注册 `v3lockpb.Lock` 与 `v3electionpb.Election`，JSON
  gateway 对应 `/v3/lock/*` 和 `/v3/election/*`。两项服务本质上复用
  client/v3 concurrency 的 KV/Lease/Watch recipe，而不是独立 raft 状态。

  提交 `eac694c` 引入匹配现有协议基线的 `server/v3@v3.7.0` generated protobuf 和
  upstream Lock/Election 实现，以 KubeBrain RPCServer 的 server-to-client adapter
  构造同进程 client，保持调用者 context、Auth metadata、TiKV revision 与现有
  KV/Lease/Watch 语义，不增加本机网络回环；client 与 peer gRPC listener 均注册两项
  服务，A355 gateway 同步注册 generated handlers。确定性测试覆盖 Lock lease 绑定、
  第二竞争者在 Unlock 前阻塞及之后接棒、Unlock 删除、Campaign、Leader、Proclaim、
  Resign，以及 Auth 开启时匿名拒绝和 root token 成功。

  双端 Auth 差分进一步发现 current `/root/etcd` 的专用 convenience API 会把内部
  匿名认证错误暴露为 gRPC `Unknown`/HTTP 500，而标准 KV Range 是
  `InvalidArgument`/HTTP 400。提交 `a393595` 增加仅包裹 Lock/Election 的错误兼容层，
  提交 `b9bd615` 让 response-header interceptor 对这两个 service 保留外层状态，
  但成功响应仍填写 cluster/member/revision/raft_term；标准 KV/Lease/Auth 错误转换
  不变。该差异现由 reference 双端测试固定，避免把更“规整”的错误码误报为兼容。

  HTTP 双端矩阵覆盖 Lock/Unlock、Campaign/Leader/Proclaim/Resign；Auth 矩阵覆盖
  anonymous Lock 精确 status/code、Authorization token Lock/Unlock，且继续覆盖
  A355 的 anonymous/authorized Range 与 AuthDisable。最终 exact image 从
  `b9bd6159d9df32dea975245617c885b2c3719b64` 的 `git archive` 构建，tag
  `kubebrain:a356-dedicated-concurrency-release`，image ID
  `sha256:7e0742e70a6d60354dc08d0257110fa264a6491cd66b9fa77e535c44d63ac2f0`，
  OCI version `0.0.0-a356.2`、Go 1.26.5/linux/amd64、TiKV、运行用户
  `65532:65532` 均匹配。

  在真实 3 PD/3 TiKV 独立 `a356-release` keyspace 上，final Pod UID
  `8db12252-6750-458d-b870-3864ad845288`，Ready、restartCount=0、只读根文件系统、
  non-root、drop ALL、RuntimeDefault seccomp，日志无 initialization failure/panic/
  fatal/segmentation/data race/storage error。专用 concurrency 与 Auth 双端矩阵各
  连续 10 轮一致，A355 核心 JSON 矩阵再连续 10 轮通过。聚焦服务测试普通 20 轮、
  gateway 合同 20 轮、完整根测试、endpoint/etcd race 及根/compat vet 全部通过。
  Watch/Observe HTTP streaming 的数天级故障 soak 仍属于 P1，不据此声明长期流稳定性。

- **Compatibility A357 HTTP Watch/Observe streaming（2026-07-20）**：对照
  `/root/etcd/server/etcdserver/api/v3rpc/watch.go` 与 grpc-gateway generated
  handlers/runtime，确认 JSON Watch 在有限请求体解码完成后会对 gRPC 请求侧执行
  `CloseSend`，但服务端必须继续保留响应侧直到 HTTP client 取消。原 KubeBrain
  `RPCServer.Watch` 把 `io.EOF` 当作整个双向流结束，导致 `/v3/watch` 只发送 created
  帧便关闭；提交 `e5a4504` 将 EOF 按半关闭处理，保留 active watches 和发送侧，直到
  stream context 取消后同步关闭并释放配额。

  单元测试固定“create -> EOF -> Put event 仍送达 -> context cancel 后退出且配额归零”；
  bufconn + 真实 HTTP server 合同测试固定 Watch 和 Election Observe 的
  `Transfer-Encoding: chunked`、`application/json`、每条换行分隔的
  `{"result":...}` envelope、base64/string-int64 编码，并在第二条消息尚未放行时读到
  第一条，证明逐帧 flush。独立兼容测试用有限 Watch body 读取 created 后执行 Put，
  校验 event/prev_kv；Observe 先读当前 leader，再 Proclaim 并读取更新。

  exact image 从 `e5a4504a15e1fea5e6417a0dd5dfecb3b25cb35d` 的 `git archive`
  构建，tag `kubebrain:a357-http-streams`，image ID
  `sha256:5d2adc05b9d963e9004aecbf3a5b0aa72323ddce327dc1e46af464fda4274056`，
  OCI version `0.0.0-a357.1`、revision、Go 1.26.5/linux/amd64、TiKV 和运行用户
  `65532:65532` 均匹配。在真实 3 PD/3 TiKV 独立 `a357-release` keyspace 上，Pod UID
  `8d9ae064-c947-477c-9dc2-2fe7e15b3f7d`，Ready、restartCount=0、只读根文件系统、
  non-root、drop ALL、RuntimeDefault seccomp，日志无 initialization failure/panic/
  fatal/segmentation/data race/storage error。

  reference etcd 双端流式差分连续 10 轮通过；候选以 `--max-watches=1` 运行，逐轮关闭
  HTTP response 后下一轮仍可创建 Watch，证明取消传播和配额释放。双端流式 race 3 轮、
  服务/gateway 聚焦 race 20 轮、既有核心与 concurrency HTTP 双端回归 3 轮、完整根
  测试和根 vet 均通过。未配置本地 `127.0.0.1:3379` 时直接运行 compat 全套 race 会按
  设计尝试 live endpoint 并 connection refused，不计为通过；本轮仅对已显式配置双端
  的目标流式场景执行 race。数天级网络故障、leader replacement 和慢消费者 soak 仍为
  P1，不能由这些确定性测试替代。

- **Compatibility A358 HTTP LeaseKeepAlive streaming（2026-07-20）**：对照
  `/root/etcd/api/etcdserverpb/gw/rpc.pb.gw.go` 的 generated
  `request_Lease_LeaseKeepAlive_0` 与
  `/root/etcd/server/etcdserver/api/v3rpc/lease.go`，确认 `/v3/lease/keepalive`
  会把同一 HTTP body 中连续的 JSON 对象逐个发送到双向 gRPC stream；body EOF 触发
  `CloseSend`，服务端处理完已接收消息后正常返回。每条请求各产生一个 newline-delimited
  `{"result":...}` chunk；未知 lease 不终止 stream，返回原 ID 与 TTL=0，而当前
  proto JSON `EmitUnpopulated=false` 会省略零值 `TTL` 字段。

  提交 `90ab9f3` 增加 bufconn + 真实 HTTP server 合同测试，以两个连续请求固定
  chunked/content-type、响应顺序、string-int64 字段、帧数和最终 EOF；真实 reference
  差分在有效 lease、未知 lease、同一有效 lease三条请求上固定 ID 顺序、有效 TTL>0、
  未知 TTL 字段省略及响应正常结束，并与 A357 Watch/Observe 场景共同执行。探测未发现
  运行时代码差异，本轮关闭的是此前缺失的跨 Lease/JSON 流式发布门禁。

  exact image 从 `90ab9f34c75f6a42996f095ff4fd2bd2cddc6833` 的 `git archive`
  构建，tag `kubebrain:a358-http-lease-stream`，image ID
  `sha256:9895064d2059955b50c53d327e4f144c0d5438456fe7ff7cecae328d17fb4c65`，
  OCI version `0.0.0-a358.1`、revision、Go 1.26.5/linux/amd64、TiKV 和运行用户
  `65532:65532` 均匹配。在真实 3 PD/3 TiKV 独立 `a358-release` keyspace 上，final
  Pod UID `82c8a068-6b1c-4382-9624-7985baccca58`，Ready、restartCount=0、只读根
  文件系统、non-root、drop ALL、RuntimeDefault seccomp，日志无 initialization
  failure/panic/fatal/segmentation/data race/storage error。精确镜像双端完整流式场景连续
  10 轮、此前候选探测 10 轮、双端 race 3 轮、generated gateway race 20 轮、完整根
  测试和根 vet 均通过。长时 keepalive 断线重连、leader replacement 与网络故障 soak
  仍属于 P1，不由有限 body 的确定性测试替代。

- **Compatibility A359 blocked HTTP Lock/Campaign cancellation（2026-07-20）**：
  对照 `/root/etcd/server/etcdserver/api/v3lock/lock.go`、
  `/root/etcd/server/etcdserver/api/v3election/election.go` 及
  `/root/etcd/client/v3/concurrency/{mutex,election}.go`，确认阻塞中的 Lock/Campaign
  收到 request context cancel 后必须用内部 client context 删除自己的租约绑定队列键。
  否则 HTTP client 虽已断开，残留 waiter 仍可能在 owner 释放后取得锁或领导权，造成
  无消费者的幽灵接棒并阻塞后续请求。

  提交 `58be8b5` 增加 bufconn 网关回归，直接阻塞 fake Lock/Campaign handler，取消
  HTTP request 后同时要求 client 收到 `context.Canceled` 且 gRPC handler context
  Done。真实双端测试为 Lock 和 Campaign 各准备 owner、canceled waiter、successor
  三个独立 lease；通过 KV prefix Count 确认 waiter 键已实际入队后才取消，要求计数从
  2 收敛到 1，再释放 owner 并确认 successor 立即取得资源，最后要求前缀归零。该形状
  固定 HTTP context、generated gateway、本机 gRPC adapter、Watch waitDeletes 和清理
  Delete 的完整链路，而不是依赖定时 sleep 推断。

  exact image 从 `58be8b575d960034eb265ff52ed045e987c7278b` 的 `git archive`
  构建，tag `kubebrain:a359-http-concurrency-cancel`，image ID
  `sha256:329d91a06c88d095227a7600402a633044f2b5ab41bf5a6c60980bb80632b5fc`，
  OCI version `0.0.0-a359.1`、revision、Go 1.26.5/linux/amd64、TiKV 和运行用户
  `65532:65532` 均匹配。在真实 3 PD/3 TiKV 独立 `a359-release` keyspace 上，final
  Pod UID `8805db6f-d1db-4df8-8ba7-b96a8606a093`，Ready、restartCount=0、只读根
  文件系统、non-root、drop ALL、RuntimeDefault seccomp，日志无 initialization
  failure/panic/fatal/segmentation/data race/storage error。精确镜像双端差分连续 20 轮
  （共 40 个 canceled waiter）通过，候选探测 20 轮、双端 race 5 轮、gateway race
  50 轮、完整根测试和根 vet 均通过。未发现运行时代码差异，本轮关闭的是此前缺失的
  阻塞 HTTP 并发请求取消清理门禁；leader replacement 与网络半断连接仍需长时 soak。

- **Compatibility A360 dedicated concurrency error matrix（2026-07-20）**：对照
  `/root/etcd/server/etcdserver/api/v3lock/lock.go`、
  `/root/etcd/server/etcdserver/api/v3election/election.go` 和真实 upstream gateway，
  固定专用 convenience API 的非直觉错误外观。Lock 使用不存在 lease、Unlock 空 key、
  Campaign 使用不存在 lease、Proclaim/Resign 缺失 leader、Leader 查询空 election，
  upstream 均返回 gRPC `Unknown`，generated HTTP gateway 均为 status 500/code 2；消息
  分别精确为 `etcdserver: requested lease not found`、`etcdserver: key is not provided`、
  `"leader" field must be provided` 和 `election: no leader`。这些接口没有沿用底层标准
  KV/Lease 的 400/404，兼容层不能擅自把错误变得更规整。

  提交 `67d504a` 增加服务层六用例回归，直接断言 adapter 返回的 gRPC code/message；
  HTTP 双端矩阵先断言 reference 的固定 baseline，再比较 KubeBrain，避免两端共同漂移
  仍误判通过。静态审计曾怀疑不存在 lease 的 NotFound 会逃过 A356 wrapper，但真实
  adapter 路径最终已映射为 Unknown，未形成运行时差异；本轮因此不扩大错误转换范围，
  只增加精确门禁，避免误伤 context cancellation、Unavailable 等应保留状态。

  exact image 从 `67d504a04befb8612bdc2fea91aa20f5d3a55267` 的 `git archive`
  构建，tag `kubebrain:a360-concurrency-errors`，image ID
  `sha256:5cc94a24af36d6729d6156febc463e0c9d7fcfc5599849629ab6e4224e7f74dc`，
  OCI version `0.0.0-a360.1`、revision、Go 1.26.5/linux/amd64、TiKV 和运行用户
  `65532:65532` 均匹配。在真实 3 PD/3 TiKV 独立 `a360-release` keyspace 上，final
  Pod UID `ed195505-e48b-44cc-a026-0d818eac9c55`，Ready、restartCount=0、只读根
  文件系统、non-root、drop ALL、RuntimeDefault seccomp，日志无 initialization
  failure/panic/fatal/segmentation/data race/storage error。精确镜像双端矩阵连续 50 轮、
  候选矩阵 20 轮、双端 race 10 轮、服务 race 50 轮、完整根测试和根 vet 均通过。

- **Compatibility A361 zero-lease Lock/Campaign sessions（2026-07-20）**：对照
  `/root/etcd/client/v3/concurrency/session.go` 与 dedicated Lock/Election server，确认
  请求 `lease=0` 时 `concurrency.NewSession` 会自动 Grant 默认 60 秒 lease；service
  随后调用 `Session.Orphan` 停止 keepalive，但不会 revoke。Lock response 只返回 key，
  不返回自动 lease ID；Campaign 的 `LeaderKey.Lease` 会暴露它。Unlock/Resign 只删除
  队列键，自动 lease 在剩余 TTL 内继续存在，随后自然过期。这是 upstream 资源生命周期，
  不能把零值误作永久无 lease 键，也不能在成功返回时擅自 revoke。

  提交 `e181c91` 增加服务回归：通过 Lock key 的 KV lease metadata 反查隐藏 lease，
  对 Campaign 校验 response lease 与键 metadata 相同；两者均要求非零、互相独立、
  GrantedTTL=60、当前 TTL>0，Unlock/Resign 后键消失但 lease 仍存活，最后显式 revoke
  避免测试泄漏。HTTP 双端矩阵执行同一流程，并先断言 reference 的完整布尔 baseline，
  覆盖 generated JSON、string-int64、KV metadata、TTL 及清理链路。

  exact image 从 `e181c91fb82356f72fef12eeaccf2a72b96d3ef7` 的 `git archive`
  构建，tag `kubebrain:a361-zero-lease-concurrency`，image ID
  `sha256:8cb3d42cd5b1e57a4ed118927554dec66067815523728f28b878f3a5a6f9e5cf`，
  OCI version `0.0.0-a361.1`、revision、Go 1.26.5/linux/amd64、TiKV 和运行用户
  `65532:65532` 均匹配。在真实 3 PD/3 TiKV 独立 `a361-release` keyspace 上，final
  Pod UID `5c605788-63a9-4599-a7dc-81fc0a0722bc`，Ready、restartCount=0、只读根
  文件系统、non-root、drop ALL、RuntimeDefault seccomp，日志无 initialization
  failure/panic/fatal/segmentation/data race/storage error。精确镜像双端矩阵连续 20 轮、
  候选矩阵 20 轮、双端 race 10 轮、服务 race 50 轮、完整根测试和根 vet 均通过；
  每轮显式回收自动 lease，未用 namespace 删除掩盖测试资源泄漏。未发现运行时代码差异。

- **Compatibility A362 zero-lease natural expiry（2026-07-20）**：A361 证明
  lease=0 会创建 60 秒 orphan session，但随后显式 revoke，尚不能证明 dedicated
  service 的 `Session.Orphan` 真正终止内部 keepalive。提交 `5a268af` 增加约一分钟的
  HTTP 双端场景：Lock/Campaign 均不调用 Unlock/Resign，也不主动 revoke；reference
  与 KubeBrain 作为并行子测试同时计时，创建后约 2 秒要求两个 TTL 都从初值下降，随后
  最多等待 75 秒，直到两个 LeaseTimeToLive 均返回 -1 且两个队列键都消失。

  首轮测试还固定一个 protobuf JSON 边界：lease 仍存在但进入最后一秒时 TTL=0，
  `EmitUnpopulated=false` 会省略 `TTL`，同时 `grantedTTL=60` 仍存在；真正过期后 upstream
  才显式返回 `TTL:"-1"` 且省略零值 grantedTTL。修正测试解析后，候选 reference
  61.05s、KubeBrain 60.17s 通过；race instrumentation 下完整双端场景约 62.15s 通过。
  这同时证明自动 lease 不会被隐藏的 keepalive goroutine 永久续租，lease timer 会原子
  删除 Lock/Election 键。

  exact image 从 `5a268af7ceae1e1cd9a6b8baea96a8658f222b6e` 的 `git archive`
  构建，tag `kubebrain:a362-zero-lease-expiry`，image ID
  `sha256:d5534f02874179d343b7873a8af7865bca65ba5e4ff67178c0fdb108bde605a1`，
  OCI version `0.0.0-a362.1`、revision、Go 1.26.5/linux/amd64、TiKV 和运行用户
  `65532:65532` 均匹配。在真实 3 PD/3 TiKV 独立 `a362-release` keyspace 上，final
  Pod UID `13b8939a-c7ba-487c-b5b6-f504b7f67e67`，Ready、restartCount=0、只读根
  文件系统、non-root、drop ALL、RuntimeDefault seccomp；完整到期负载后日志仍无
  initialization failure/panic/fatal/segmentation/data race/storage error。精确镜像最终
  场景 KubeBrain 60.18s、reference 61.05s 通过，完整根测试、根 vet 和真实双端 race
  均通过。未发现运行时代码差异；多副本 leader replacement 跨越自动 lease 到期边界
  仍属于后续故障 soak。

- **Compatibility A363 zero-lease expiry across leader replacement（2026-07-20）**：
  提交 `b53d993` 增加真实三副本破坏性门禁，把 A362 的自然到期场景跨越一次当前 leader
  replacement。测试经 HTTP Lock/Campaign dedicated service 创建两个 lease=0 session，
  从返回键 metadata 和 Campaign leader 分别取得两个独立 lease ID，确认 granted TTL=60
  且倒计时到不高于 30 秒后，通过每个 Pod 的 `/election` 证明受害者 `IsLeader=true`，再
  删除该 Pod并等待 StatefulSet 恢复。successor 必须能查询原 lease/键，并在最多 75 秒内
  让两个 LeaseTimeToLive 都返回 -1、两个队列键都消失；测试失败时才执行 revoke 兜底。

  首轮候选场景曾按“换主不能刷新 TTL”设计，45 秒后仍未过期。对照 `/root/etcd`
  `server/lease/lessor.go` 后确认这不是运行时差异：upstream `lessor.Promote(electionTimeout)`
  会对无 remaining-TTL checkpoint 的 lease 调用 `refresh`，从 granted TTL 加 election
  timeout 重建 expiry；KubeBrain 同样从 granted TTL 恢复这类短 lease。门禁因此固定真正
  的兼容不变量：换主前 TTL 已降至约 30，换主后只允许一次性恢复到不高于 65 秒，随后在
  没有第二次换主时必须自然到期，不能被隐藏 keepalive 永久续租。修正后的候选场景
  90.48 秒通过，race instrumentation 下 91.52 秒通过。

  exact image 从 `b53d99376eb0c08c50e4a68df8d89bda6a1a2f48` 的 `git archive`
  构建，tag `kubebrain:a363-zero-lease-failover`，image ID
  `sha256:355be78f1400bb5bf9bef7d748dea86eb95f88225ec7ec12755b9ceac0ab5d00`，
  OCI version `0.0.0-a363.1`、revision、Go 1.26.5/linux/amd64、TiKV 和运行用户
  `65532:65532` 均匹配。在真实 3 PD/3 TiKV 独立
  `a363-zero-lease-failover-final` keyspace 上，精确镜像最终场景 90.72 秒通过：删除前
  `kubebrain-2` UID `2a6960d8-8bdf-4ccc-9413-113fc2a09624` 且为当前 leader，替换后 UID
  `05b92b4e-bf02-49cc-b8c0-7426a633de06`；三副本 Ready、restartCount=0、运行时 digest
  `sha256:672154cf2cfe166d95f4b13b57d858f3625e0e8e8246dfa4b8ee9cfc3d53db33`
  一致，只读根文件系统、non-root、drop ALL、RuntimeDefault seccomp 生效，完整负载后日志
  无 initialization failure/panic/fatal/segmentation/data race/storage error。完整根测试、
  根 vet、兼容子模块 vet 和真实 race 均通过；未发现需要修改的运行时代码。

- **Compatibility A364 repeated zero-lease leader replacement（2026-07-20）**：
  提交 `7c63981` 将 A363 单轮场景抽成按周期运行的共享门禁，并新增独立破坏性变量控制的
  两轮当前 leader replacement 测试。两个自动 session lease 每轮都必须先从上轮 promotion
  后的约 60 秒重新下降到不高于 30 秒，故障命令才重新查询全部 Pod `/election`、证明本轮
  受害者 `IsLeader=true` 并删除它；successor 恢复后两个 lease 都必须比故障前增加超过
  20 秒但不高于 65 秒，且 granted TTL 仍为 60。第二轮完成后停止注入故障，最多等待
  75 秒要求两个 lease 同时返回 TTL=-1 且 Lock/Election 键消失。单轮和双轮分别使用
  `KUBEBRAIN_ZERO_LEASE_EXPIRY_FAILOVER_COMMAND` 与
  `KUBEBRAIN_ZERO_LEASE_EXPIRY_REPEATED_FAILOVER_COMMAND`，常规 CI 缺少对应变量时独立 Skip。

  候选真实 TiKV 场景 121.42 秒通过，两轮分别删除 leader `kubebrain-2` 与
  `kubebrain-1`；race instrumentation 121.29 秒通过，两轮分别删除 leader
  `kubebrain-0` 与 `kubebrain-2`。每轮等待 TTL 再次下降本身证明 promotion 后没有持续
  keepalive，最后自然到期则证明连续 promotion 只延长故障恢复窗口，不会永久保留队列键。

  exact image 从 `7c639814fb19b636394006a9205b395a3edb9a5f` 的 `git archive`
  构建，tag `kubebrain:a364-repeated-zero-lease-failover`，image ID
  `sha256:5f5bfc4abb502ebd088ac866beb8d15bb6a267a37dec12969070ea30afe32e7a`，
  OCI version `0.0.0-a364.1`、revision、Go 1.26.5/linux/amd64、TiKV 和运行用户
  `65532:65532` 均匹配。在真实 3 PD/3 TiKV 独立
  `a364-repeated-zero-lease-final` keyspace 上，精确镜像最终场景 121.48 秒通过：第一轮
  删除 `kubebrain-0` UID `267a375d-d51d-42c8-bb7a-d7979afea1e0`，第二轮删除
  `kubebrain-2` UID `d5a67335-af80-4144-9775-3fe17e28b0b0`，两者删除前均由
  `/election` 证明为当前 leader；最终三个 Pod UID 分别为
  `77e8d7f2-ee15-4849-9e6b-121977e00e57`、`4285f882-f9ee-4ea3-96d7-8241a013f331`、
  `faec2b6c-dc14-4435-ba90-1b1789416a50`，均 Ready、restartCount=0，运行时 digest
  `sha256:f61d15df222d58fcb062d655e2e3cc28ef5edc561ded7c5ac8395f95044b4077`
  一致，只读根文件系统、non-root、drop ALL、RuntimeDefault seccomp 生效，完整负载后日志
  无 initialization failure/panic/fatal/segmentation/data race/storage error。完整根测试、
  根 vet、兼容子模块 vet 和真实 race 均通过；未发现需要修改的运行时代码。

- **Compatibility A365 HTTP concurrency authorization（2026-07-20）**：提交
  `9b9456f` 增加 Auth 启用下的 Lock/Election HTTP 双端差分，reference 使用
  `/root/etcd` 源码构建的 3.8.0-alpha.0、Git SHA `d947b2086`。场景创建非 root
  `alice`，只授予 `/a365/concurrency/allowed/` prefix READWRITE：显式 lease 下
  Lock/Unlock、Campaign/Leader/Proclaim/Resign 必须全部 HTTP 200；对 denied prefix 的
  Lock、Campaign、Leader 必须 HTTP 500、gRPC JSON code 2，permission denied 消息与
  upstream 一致。root 随后撤销 allowed 权限，已签发 alice token 必须立即被同一路径拒绝；
  重新授权后同一 token 必须恢复 Lock/Unlock；最后修改 alice 密码，旧 token 必须返回
  HTTP 500/code 2/`etcdserver: invalid auth token`。这固定 dedicated service 经内部
  client 执行 Txn/Range/Delete 时仍保留外部调用者身份及实时 auth revision，而不是以
  KubeBrain 服务身份绕过 key-range RBAC。

  候选双端场景单轮 1.83 秒通过，连续 20 轮 39.16 秒通过，race 10 轮 28.10 秒通过；
  未发现运行时代码差异。exact image 从
  `9b9456fefc0e9b36e36ebb53e61718c807075d5e` 的 `git archive` 构建，tag
  `kubebrain:a365-concurrency-http-authz`，image ID
  `sha256:2b8003855c9e5e4e4c37f45cc9e94a81b601662b998c6b73defa4e767688d9c5`，
  OCI version `0.0.0-a365.1`、revision、Go 1.26.5/linux/amd64、TiKV 和运行用户
  `65532:65532` 均匹配。

  在真实 3 PD/3 TiKV 独立 `a365-concurrency-authz-final` keyspace 上，三副本精确镜像
  经 NodePort 随机落到 leader/follower 的最终矩阵 20 轮 42.58 秒通过，race 10 轮
  21.24 秒通过。三个 Pod UID 分别为 `879ca4ee-0c5e-450f-bf98-4861bcba747e`、
  `5bfeccf9-b644-4533-988c-9b8ed8ab98d9`、`491cf57e-86d6-496c-acc6-fa2bdd7375ee`，
  均 Ready、restartCount=0，运行时 digest
  `sha256:ab2eda4a054481ccf80c508779305fe668752fca7a3751afe298626665f786c0`
  一致，只读根文件系统、non-root、drop ALL、RuntimeDefault seccomp 生效，完整负载后日志
  无 initialization failure/panic/fatal/segmentation/data race/storage error。完整根测试、
  根 vet、兼容子模块 vet 和真实 race 均通过。

- **Compatibility A366 HTTP Election Observe auth change（2026-07-20）**：提交
  `aff0ed7` 增加 upstream 双端差分：已有 Observe 在 role 撤权或用户改密后仍应继续接收
  Proclaim；使用旧 auth revision、当前无权限或已失效 token 新建 Observe 时，HTTP 契约为
  200 且空响应体。单副本 TiKV 连续 20 轮和 race 10 轮通过，但首个 A366.1 三副本精确
  镜像稳定暴露 follower 差异：已有 Observe 以 EOF 关闭，leader 日志为
  `etcdserver: user name is empty`。

  根因是 Watch create 仅在 ingress follower 本地鉴权，传给 leader proxy 的 context 未携带
  caller token。提交 `7b92188` 使用既有 `forwardAuthToken` 为该 watch 的首次转发及后续重连
  保留身份，并增加 follower proxy token 回归测试；leader 只在 watch 建立时鉴权，因此后续
  撤权/改密不会追溯关闭已建立流，和 etcd 一致。核心包、race、endpoint、全仓 vet 均通过；
  全仓测试初次运行仅因仍占用 12379/12380 的 reference etcd 被 endpoint 测试误连而失败，
  清理 reference 后受影响包通过。

  最终 exact image `kubebrain:a366-election-observe-auth` 从 `7b92188` 干净归档构建，image ID
  `sha256:d91cd0a507c8f0e0133a5dc6ecc2b09c425a8f77e0e03cd8a580e28959f0ede1`，OCI version
  `0.0.0-a366.2`、revision `7b92188ef9e0f87829ac8f97a6fbdf356ea7dcaf`、Go
  1.26.5/linux/amd64、TiKV、运行用户 `65532:65532`。真实 3 PD/3 TiKV、三 KubeBrain
  副本、NodePort 随机 leader/follower 的最终矩阵连续 20 轮 56.12 秒、race 10 轮 28.31 秒
  通过。Pod UID 为 `629bc0f1-2f64-420b-9c63-7354a934ef48`、
  `8a4a1615-0196-47a1-948d-0387c50bd33e`、`c9556aa7-903f-40c1-acf1-d0b6a7bf4804`，
  均 Ready、restartCount=0，runtime digest 一致。负载后无 proxy watch error/panic/fatal/data
  race；auth 启用时 follower 的 unauthenticated `Maintenance/Status` 健康探测仍产生
  `user name is empty` 告警，属于后续需消除的内部探测噪声，不影响本轮请求正确性。

- **Compatibility A367 Auth-safe peer readiness（2026-07-20）**：A366 三副本验证发现
  follower 的共享 leader client 每次 `checkClientConn` 都用未认证的
  `Maintenance.Status` 探测传输；Auth 开启后 leader 正确拒绝它，虽然旧代码把该应用层错误
  当作 transport ready，但每秒持续产生 `etcdserver: user name is empty` 告警，掩盖真实认证
  故障。提交 `ba21258` 改为 peer listener 已注册且不经过 etcd Auth 的标准
  `grpc.health.v1.Health/Check`，并要求状态明确为 `SERVING`。这同时证明 TCP、mTLS、HTTP/2、
  service routing 和服务初始化完成；不签发内部 root token，`NOT_SERVING`、未注册服务和连接
  错误均保持 unavailable。单元测试覆盖 SERVING/NOT_SERVING，proxy 包 race、完整根测试和
  根 vet 通过。

  exact image `kubebrain:a367-peer-health-readiness` 从 `ba21258` 干净归档构建，image ID
  `sha256:d1bf1b6ada0e2fabcff0118e042debb7e5fd95b5bc33a1c2e6dc4a14c6750e36`，OCI version
  `0.0.0-a367.1`、revision `ba212586f26c8b0856c128be16a5c4dc69b5cc83`、Go
  1.26.5/linux/amd64、TiKV、运行用户 `65532:65532`。在独立 3 PD/3 TiKV、隔离
  `a367-peer-health-final` keyspace 和三 KubeBrain 副本上启用 Auth 后，经 NodePort 发出
  30 次 root Put；两个 follower 均实际记录 `forward put`，持续 12 秒的 leader readiness
  探测无 `Maintenance/Status`、`user name is empty`、panic/fatal/proxy error。三个 Pod UID
  为 `a1d1acfe-eba9-4368-8da0-54c18835ed5f`、
  `98832d4a-fc5a-4644-9d78-a781acef25d9`、`708793a1-9964-44da-b44f-76194b3706c2`，
  均 Ready、restartCount=0、runtime digest 一致，non-root、只读根文件系统、drop ALL 和
  RuntimeDefault seccomp 生效。启动首秒仅有 headless Service DNS 尚未发布时的预期重试，
  DNS 就绪后两个 follower 均通过 Health Check 连接 leader。

- **Compatibility A368 authenticated clientv3 AutoSync（2026-07-20）**：提交
  `7b1d412` 补齐现有覆盖空洞：此前 Auth 差分只证明普通用户可单次 MemberList，Sync 测试
  则只在 Auth 关闭时显式调用。新双端场景为无管理权限的 alice 仅授予探测 prefix READ，
  从无 scheme 的 seed endpoint 创建 `AutoSyncInterval=100ms` client，要求后台自动将 endpoint
  精确替换为 MemberList 的非 learner ClientURLs，并通过替换后的 resolver 完成授权 Range。
  `/root/etcd` 3.8.0-alpha.0 与本机单副本 KubeBrain 单轮通过、连续 20 轮 21.32 秒通过，
  race 10 轮 12.49 秒通过；未发现运行时代码差异，兼容子模块 vet 通过。

  exact image `kubebrain:a368-authenticated-autosync` 从 `7b1d412` 干净归档构建，image ID
  `sha256:c8292bf2f5e0a3e13905ecfc9f4d4cfdf1606aa600e181d7d5e22b73b3e25583`，OCI version
  `0.0.0-a368.1`、revision `7b1d412d9282d70605917be5dcc77f17fa24b784`、Go
  1.26.5/linux/amd64、TiKV、运行用户 `65532:65532`。在独立 3 PD/3 TiKV、隔离
  `a368-authenticated-autosync-final` keyspace 和三 KubeBrain 副本内运行静态 compat probe：
  每轮从 `kubebrain:3379` ClusterIP seed 启动，AutoSync 必须改成三条稳定 Pod DNS ClientURL，
  再经这些地址完成普通用户 Range；连续 20 轮约 36 秒全部通过。

  三个 Pod UID 为 `b61e2521-a179-4a02-8a40-d0baec2299b4`、
  `66df325d-70d8-4dfc-ab07-3e6a8330a872`、`0faafb80-d27f-4105-b01c-26b1a4370e5e`，
  均 Ready、restartCount=0、runtime digest 一致。负载日志无 user-empty/panic/fatal/data race/
  storage error；Parallel StatefulSet 启动最初两秒存在 headless Service 尚未发布 leader Pod DNS
  的预期 unavailable 重试，DNS 就绪后恢复且不影响最终 readiness 或 AutoSync。生产控制面仍
  必须为所有副本注入相同完整 `--initial-cluster`，否则 MemberList 的 fallback 集合不完整，
  AutoSync 会合法但危险地缩减客户端 endpoint 集合。

- **Compatibility A369 generated nested Txn response headers（2026-07-21）**：对照
  `/root/etcd/server/etcdserver/txn/txn.go` 的 `newTxnResp`/`executeTxn` 路径确认：每层
  nested `TxnResponse` 都分配非空 `ResponseHeader`，但只有顶层 Txn 写入最终 revision；
  nested wrapper 的 header revision 保持零，叶子 Range/Put/Delete 仍携带提交 revision。
  修复前 KubeBrain 的 atomic plan 与 staged executor 都递归盖写 nested header，因此客户端
  递归检查响应时可观察到差异。提交 `3d1f332` 使两条路径都创建零值 nested header，并把
  `stampTxnResponseHeaders` 限定为顶层；单元测试同时固定 nested header 非空且 revision 为零。

  新生成式差分使用固定 seed 369，每轮执行 32 组相互隔离的三层 Txn，覆盖三层 compare
  成功/失败组合、未选择分支、Put/Delete PrevKV、prefix Range、missing-key compare、递归响应
  顺序/header/KV revision 和最终状态。本机 Badger 对 `/root/etcd` 连续 20 轮及 race 10 轮
  通过。提交 `32aef9d` 进一步按被测 Txn revision 归一化提交版本，允许 HA 集群在 seed 与
  被测 Txn 之间合法推进无关的内部 revision，同时仍要求所有叶子响应与新 KV 精确归属于
  同一提交 revision，nested wrapper 必须保持零。

  exact runtime image `kubebrain:a369-nested-txn-headers` 从 `3d1f332` 干净 Git archive
  构建，image ID `sha256:3b9bff628dfd61fff9f920817b7643af3f7e66011207250041473258d186b79c`，
  OCI version `0.0.0-a369.1`、revision `3d1f332e7b3385a49e6e204ed6b2d7454d522d1b`、
  Go 1.26.5/linux/amd64、TiKV、运行用户 `65532:65532`。在独立 3 PD/3 TiKV、隔离
  `a369-nested-txn-headers-final` keyspace 和三 KubeBrain 副本上，真实 TiKV 差分连续
  20 轮通过，随后显式 3 轮复验 27.294 秒通过；
  三 Pod Ready、restartCount=0、runtime digest 一致，日志无 panic/fatal/data race/storage
  error。三 Pod UID 为 `3e58bae8-bd3b-4b80-9fec-91da0bda666d`、
  `8efd33a0-b450-4b7a-9a80-4c7b357b16a0`、`99149032-7de7-4177-9025-97b826685de4`。

- **Maintenance A370 per-member NOSPACE alarm set（2026-07-21）**：对照
  `/root/etcd/server/etcdserver/api/v3alarm/alarms.go` 的两层 map 发现，upstream 可为
  同一 AlarmType 同时保存多个 member ID；KubeBrain 从 A333 延续的单 owner 元数据会让
  第二次 ACTIVATE 返回首个 owner、GET 丢失第二个成员，并在解除首个成员时错误清空整个
  tenant alarm。提交 `005fc49` 将同一内部键升级为严格递增的版本化 member 集合：单成员
  继续使用旧 8 字节编码，多成员使用 tag+有序 uint64，旧 1 字节 marker 和 8 字节 owner
  均可直接读取。ACTIVATE 以 exact-value CAS 幂等追加请求成员，DEACTIVATE 只移除指定成员；
  只要集合非空，tenant-global NOSPACE 写入门禁和指标就保持 active。

  不确定提交协调现按目标成员是否存在判断 ACTIVATE 是否完成、是否缺失判断 DEACTIVATE
  是否完成；并发 CAS 冲突重试，不会覆盖其他成员。Maintenance Alarm GET 按 member ID
  稳定顺序返回全部 NOSPACE，Status 也按 upstream 形状追加全部 active AlarmMember 字符串。
  单元/race 覆盖 32 路并发激活、重复操作、逐成员解除、旧格式迁移、非法集合编码和
  commit-then-uncertain/uncommitted-uncertain。新增 raw gRPC 差分依次验证双成员激活、重复
  激活、GET 两成员、逐一解除及重复解除；本机 Badger 对 `/root/etcd` 普通 20 轮和 race
  10 轮全部通过，backend 与 server 聚焦 race、根 `go vet ./...` 通过。

  exact runtime image `kubebrain:a370-member-alarm-set` 从 `005fc49` 干净 Git archive
  构建，image ID `sha256:bcbb2c82765ffdd634c9f83dcf2a914ac6c0a971b570f304bd401a3b2bf71b36`，
  OCI version `0.0.0-a370.1`、revision `005fc49d7f77549248121328f7b85f9ff6990963`、
  Go 1.26.5/linux/amd64、TiKV。在独立 3 PD/3 TiKV、隔离
  `a370-member-alarm-set-final` keyspace、1 GiB quota 和三 KubeBrain 副本上，真实 TiKV
  差分连续 20 轮通过。三 Pod UID 为 `f1de6f6e-e439-4e6a-bc8b-2370230db75c`、
  `e19e6fc2-e38f-4554-8cbe-5deaa63f632b`、`fb2a189c-1a4f-4dea-8173-f176cd068085`，
  均 Ready、restartCount=0、runtime digest
  `sha256:0a2977169a8eef5eb528f122a1d0a9b54818c713f55a33f087206f204e2bd4be`，日志无
  panic/fatal/data race/storage error。全仓测试的本次相关包通过；完整 `go test ./...`
  受运行中的 reference etcd 占用测试固定端口，以及用户未提交 `go.mod` 将 client 变为
  3.8.0-alpha.0、既有 Cilium 测试仍固定 3.7.0 两项外部状态阻断。历史 A330/A333 的
  “任意 member 多告警不支持”限制至此被取代；CORRUPT 仍由 TiKV/PD 完整性信号和 DBaaS
  管理面处理，不在数据面伪造告警。

- **Maintenance A371 explicit zero alarm member（2026-07-21）**：继续对照
  `/root/etcd/server/etcdserver/apply/backend.go:Alarm` 与
  `api/v3alarm/alarms.go:Activate`，确认手工
  `Alarm(ACTIVATE, NOSPACE, MemberID=0)` 会把合法零 ID 原样持久化并返回；KubeBrain
  此前把公开 RPC 的零值替换成 backend identity，导致后续 `DEACTIVATE(0)` 返回成功空
  列表且告警残留。修复前真实 A370 TiKV endpoint 稳定返回本地 ID `231094427`，与
  reference 的 activate/list/disarm 均为 `0` 形成完整差异。

  提交 `08cd555` 使 `ArmNoSpace` 保留请求的精确 ID，并只把原始一字节 legacy marker
  映射为本地稳定 ID；合法八字节零 owner 在读取、集合追加、不确定 disarm 协调中均保持
  为零。自动超限不依赖公开零值约定，继续由 `activateNoSpace()` 显式传入稳定非零 backend
  ID。backend/RPC 单元测试分别固定显式零 owner 的 activate/list/disarm 和自动路径身份。
  新 raw gRPC 双端回归固定零 member 完整生命周期；本机 Badger 的零 member + A370 多成员
  联合差分普通 20 轮、race 10 轮通过，backend/server 聚焦 race、完整 `go test ./...`
  与 `go vet ./...` 通过。A370 记录中的全仓失败由当时 reference etcd 占用 endpoint 测试
  固定端口造成；停止 reference 后完整套件通过，`go.mod` 用户改动本身不是该失败原因。

  提交 `92cd404` 同时修正旧跨副本测试：该测试改用显式 `MemberID=424242` 验证共享持久
  owner，不再把“省略 member 的手工请求必须返回非零”误写成协议约束。exact runtime image
  `kubebrain:a371-zero-alarm-member` 从 `08cd555` 干净 Git archive 构建，image ID
  `sha256:1369401422c2c473be7e50341288823a5a7e761d87def5e6ae05e9ceb224e4a2`，
  OCI version `0.0.0-a371.1`、revision `08cd555ffafa8505bd434f2838357ff599440460`、
  Go 1.26.5/linux/amd64、TiKV。在独立 3 PD/3 TiKV、1 GiB quota、隔离
  `a371-zero-alarm-member-final` keyspace 和三 KubeBrain 副本上，零 member + 多成员联合
  差分 20 轮、三条 Pod 直连 endpoint 的跨副本 activate/list/disarm 20 轮、完整 capped
  state 差分 20 轮全部通过。

  三 Pod UID 为 `e099a134-2bb1-4ce5-a90e-63098d9a5a17`、
  `2da79818-a35e-49f3-a2d3-578b3c22d688`、`08e38805-bea7-4297-bec9-1d46702f459f`，
  均 Ready、restartCount=0、runtime digest
  `sha256:14a252e90a0e2cf626942e9c1f55c2225d147b607e2a4e7b80643ce1259f09fb`，日志无
  panic/fatal/data race/storage error。至此 NOSPACE 支持零值、任意显式 ID 和多 member
  集合；剩余 Alarm 边界仅为 TiKV/PD 与管理面负责的 CORRUPT，以及不适用于 TiKV 的
  bbolt fragmentation。

- **Maintenance A372 manual NOSPACE without configured quota（2026-07-21）**：对照
  `/root/etcd/server/etcdserver/apply/uber_applier.go:restoreAlarms` 与
  `apply/capped.go` 确认，持久 NOSPACE 会安装 capped applier，与 backend quota 数值
  无关：手工 ACTIVATE 必须成功并阻断 Put、含 Put 的 Txn 和 LeaseGrant，同时 Range、
  只读 Txn、DeleteRange 与 DEACTIVATE 保持可用。KubeBrain 此前在
  `--quota-backend-bytes=0` 时直接以 FailedPrecondition 拒绝 ACTIVATE，后续三类写入均
  错误成功；新增双端回归在 A371 镜像上完整复现该差异。

  提交 `746c92f` 解除 `ArmNoSpace` 与逻辑用量配置的耦合；quota=0 的 `QuotaStatus`
  仍读取持久 alarm key，但不初始化或伪造 usage。统一 `TxnApply` 在用量计数条件之外先
  对有效用户 Put 检查告警，单键 Put 的 legacy Create/Update 快路径在 RPC leader/auth
  fence 后执行同一检查；Delete-only 和内部元数据写不受影响。LeaseGrant 与含 Put Txn
  继续复用原有 `QuotaStatus` 门禁。不可达的 `ErrQuotaDisabled` 及 FailedPrecondition 映射
  已删除。由于 quota=0 不再跳过 alarm store，storage unavailable 的 `/health` 按既有
  alarm-first 契约返回 `ALARM ERROR`，对应回归同步固定。

  本机 Badger 双端差分普通 20 轮、race 10 轮通过，backend/server 聚焦 race、完整
  `go test ./...` 与 `go vet ./...` 通过。exact runtime image
  `kubebrain:a372-manual-alarm-no-quota` 从 `746c92f` 干净 Git archive 构建，image ID
  `sha256:77309e6aa1f92c0d5f9752bbded78b7219c2d2161efe3e5b60aab78f4a2c1790`，
  OCI version `0.0.0-a372.1`、revision `746c92fdabee5570586a61e6cb533e7c78935bc9`、
  Go 1.26.5/linux/amd64、TiKV。

  在独立 3 PD/3 TiKV、隔离 `a372-manual-alarm-no-quota-final` keyspace、明确没有
  `--quota-backend-bytes` 的三 KubeBrain 副本上，完整十步手工 capped-state reference
  差分连续 20 轮通过；三条 Pod 直连 endpoint 的跨副本 activate/list/wrong-disarm/
  blocked-Put/correct-disarm 场景连续 20 轮通过。三 Pod UID 为
  `733e652f-82d4-4a91-af36-5c2b474ab469`、`8a83fbf0-98de-4722-8c13-ff9d7f9d8daf`、
  `d2e741d4-0f6a-4332-9e00-d6b4e349c7c8`，均 Ready、restartCount=0、runtime digest
  `sha256:6418808055474914d6f998871278fbc286c25b625db523c0aef27c80ba3eb20e`，日志无
  panic/fatal/data race/storage error。

- **Maintenance A373 capped operation matrix（2026-07-21）**：继续对照
  `/root/etcd/server/etcdserver/apply/capped.go` 和 `storage/quota.go` 审计 active
  NOSPACE 的完整入口。上游 capped applier 仅覆盖 Put、顶层任一分支含 Put 的 Txn 和
  LeaseGrant；Range、只读 Txn、DeleteRange、delete-only Txn、LeaseTimeToLive、
  LeaseRevoke 与 alarm disarm 必须继续可用。KubeBrain 的 RPC 门禁与该矩阵一致，
  Lock 和 Election recipe 使用预先创建的 lease 时也不能通过内部 Txn Put 绕过告警。
  上游 `costTxn` 不递归计算 nested Txn Put；KubeBrain 保留递归拒绝以避免生产配额绕过，
  不复制该上游保护缺口。

  `alarm_without_quota_differential_test.go` 从十步扩展为十九步：告警前创建两条 lease，
  告警后增加 failure-branch Put、TTL 查询、Mutex Lock、Election Campaign、两次 revoke
  以及 delete-only Txn。官方 concurrency 包会把底层 `ResourceExhausted` Txn 错误包装
  为普通 Go error，因此 recipe 层 `status.Code` 为 `Unknown`；原始 Put/Txn/LeaseGrant
  仍精确断言 `ResourceExhausted`。A372 原镜像和隔离 keyspace 上，真实 TiKV 与参考 etcd
  差分 20 轮、race 10 轮通过；独立兼容套件、根模块 `go test ./...` 和 `go vet ./...`
  通过。首次并行运行完整套件与告警测试导致共享 keyspace 在告警窗口内按预期拒写，
  串行隔离后通过。无运行时代码变化，未构建或发布新镜像。

- **Maintenance A374 NOSPACE startup metric recovery（2026-07-21）**：继续审计
  TiKV quota 的故障与重启路径，发现 `EnsureQuotaInitialized` 在持久 usage 未超过 quota
  时无条件发布 `quota.nospace=0`，没有检查仍存在的 sticky/manual alarm。写门禁会继续
  fail closed，但 leader 重启后告警 gauge 会漏报，直到后续 Status/QuotaStatus 请求偶然
  刷新。提交 `a950973` 让未超额启动分支读取 tenant alarm key：存在时发布 1、缺失时
  发布 0、读取错误时保持启动失败；该读取只发生在 quota 初始化，不增加请求热路径开销，
  也不自动解除低用量手工 alarm。

  同轮补齐 TiKV 不确定提交的 quota 原子性证据：commit-then-uncertain 的双 Put Txn 最终
  两个 key、同一 revision 和精确 logical usage 一起可见；uncommitted-uncertain 则两个 key、
  revision 和 usage 一起保持未提交，alarm 均不被伪造。三项聚焦测试普通 20 轮、race
  10 轮通过，完整 `go test ./...` 与 `go vet ./...` 通过。

  exact runtime image `kubebrain:a374-nospace-startup-metric` 从 `a950973` 干净 Git
  archive 构建，image ID
  `sha256:1009134640a8a8c51a95a6b4934f16343a74a0acaac48ef12f66369d46bb2dc8`，OCI
  version `0.0.0-a374.1`、revision
  `a950973508c1adc45d1da8e02eaf074d3f176c9a`、Go 1.26.5/linux/amd64、TiKV、运行
  用户 `65532:65532`。在独立 3 PD/3 TiKV、隔离 `a374-nospace-startup-metric`
  keyspace 和 1 GiB quota 上，以 owner 41844 手工激活 alarm 后滚动重建全部三副本；在
  未调用 Status/QuotaStatus 前，新 leader 首次 metrics 已为 `quota_nospace=1`，alarm
  owner 持久不变，Put 返回 ResourceExhausted，Range/Delete 仍成功。新 Pod UID 为
  `37d590e3-9860-48ff-b07e-abd375b9f077`、`df1253f1-b551-4151-beff-4f7e79c84494`、
  `4879a37a-32d6-4ce7-a29d-06e1982ee645`，runtime digest
  `sha256:dc2246071b9416c450c15dec11bbe3d29a06952ea2505a9a570368a500572fa2`，均 Ready、
  restartCount=0；disarm 后 gauge=0 且 Put 恢复，日志无 quota initialization error、
  panic/fatal/data race/storage error，PD/TiKV 3+3 Ready。

- **Operations A375 quota metric convergence across replicas（2026-07-21）**：A374 的
  真实运行证据显示 tenant-global quota gauge 仍由执行初始化或 alarm mutation 的本地
  进程更新：activate 后 followers 缺 series/保持 0；更危险的是旧 leader 原地失去
  leadership 后仍存活时会保留 1，新 leader disarm 后 PromQL `max(quota_nospace)` 会
  永久假告警，而 `min` 又会漏报 active alarm。提交 `d1440a0` 在每个 serving 副本启动
  立即并每 15 秒调用共享 `QuotaStatus` 刷新 usage/quota/nospace，单次读取以 5 秒独立
  deadline 截断；正常的首次 quota-uninitialized 窗口不计故障，其余错误累计
  `quota.refresh.err`。刷新 goroutine 绑定 server context，Close 等待其退出，避免关闭后
  访问 TiKV。

  `deploy/production/monitoring.yaml` 新增四项精确规则：持续 NOSPACE critical、logical
  usage 超过 90% 十分钟 warning、三副本 series 少于 3 或值不一致超过一分钟 warning，
  以及十分钟内 refresh error warning。manifest 测试固定表达式、for 和 severity；循环
  回归固定立即/周期执行、取消后无残留调用和阻塞 storage read 在 5 秒预算内退出。聚焦
  普通 20 轮、race 10 轮、完整 `go test ./...` 与 `go vet ./...` 通过。

  首个 A375 候选镜像在部署前 provenance 核验发现手工输入了不存在的完整 SHA
  `d1440a0d...`，已拒绝使用并从同一干净 archive 以 `git rev-parse` 的真实 SHA 重建。
  最终 exact image `kubebrain:a375-quota-metric-convergence` image ID
  `sha256:bc48d8ff5c942a8581eadceddeb107a625fc7ec2ef5ad364caf4d962bf568e34`，OCI
  version `0.0.0-a375.1`、revision
  `d1440a066981575954644de9a844deab5cbc5e04`、Go 1.26.5/linux/amd64、TiKV、运行
  用户 `65532:65532`。

  在独立 3 PD/3 TiKV、1 GiB quota、隔离 `a374-nospace-startup-metric` keyspace 上，
  owner 41845 activate 后 gauges 从 `1/0/0` 在一个周期内收敛 `1/1/1`。随后向当前 leader
  PID 1 发送 STOP，等待其他副本接管后 CONT，使旧 leader 在不重建 Pod 的情况下以 stale
  1 恢复；新 leader disarm 后立即观测 `1/0/0`，20 秒后自动收敛 `0/0/0`，alarm list
  为空且 Put 恢复。Pod UID 为 `00f6fe22-3f09-4c25-b4b9-1c9161a7f326`、
  `3ecb5249-4d91-4f3f-95d5-bed62f92ae1b`、`68b3d334-f7f2-4fd4-ad8d-e9ae1c6f6315`，
  runtime digest `sha256:2cd9f031a5fa9ba3e1ae420ec5f0fd3ff24f38c90f776e1d5ecfcb47e0d90fb3`，
  全部 Ready、restartCount=0；日志无 quota refresh/initialization error、panic/fatal/
  data race/storage error，PD/TiKV 3+3 Ready。

- **Maintenance A376 bounded quota usage rebuild（2026-07-21）**：继续审计 quota
  在线启用与重新启用路径，发现 `EnsureQuotaInitialized` 通过无 limit 的 `Range` 一次性
  物化 tenant 全部存活 key/value；在生产 500 GiB TiKV volume 和百万键租户上，该启动
  路径的内存随数据集线性增长，可能在配额发布滚动中 OOM。提交 `c1fb664` 改用现有
  partition-parallel、带 backpressure 的 `RangeStream(..., keysOnly=false)`，逐响应累加
  logical usage，同时保留 int64 overflow、固定 revision、fence 和原子 metadata commit
  语义。回归测试包装 scanner，明确首次初始化只调用一次 stream API、从不调用无界
  `Range`，持久 clean 后重复初始化不再扫描；聚焦测试普通 20 轮、race 10 轮、完整
  `go test ./...` 与 `go vet ./...` 通过。

  exact runtime image `kubebrain:a376-stream-quota-rebuild` 从
  `c1fb6648b3773407022d1e9f955493d251e12a8e` 干净 Git archive 构建，image ID
  `sha256:8cec6f228b1171578a232d8b5587fa1e941ee116e20c4df7c40b46aca1ace49d`，OCI
  version `0.0.0-a376.1`、revision 与二进制版本一致、Go 1.26.5/linux/amd64、TiKV、运行
  用户 `65532:65532`。在独立 3 PD/3 TiKV 和隔离 keyspace
  `a376-stream-quota-rebuild` 上，先以无 quota 的 A376 写入 63,874 个存活键；keys-only
  独立核算 key bytes 1,415,079、固定 value bytes 2,043,968，总 logical usage
  3,459,047 bytes。随后在线增加 1 GiB quota 并滚动三个副本，全部 Ready、
  restartCount=0，三副本 `quota_logical_usage_bytes` 均精确为 3,459,047、
  `quota_backend_bytes=1073741824`、`quota_nospace=0`；新进程 VmHWM 为 128-145 MiB。
  重建后 count 仍为 63,874，endpoint health 与 Put/Get/Delete 通过，Pod UID 为
  `d6415085-92d3-4356-96af-e2825ef36af2`、
  `33241036-e643-479a-8d21-11881c4dc63c`、
  `3f4e2f5a-fac3-4827-a513-ed5e78c1ee16`，runtime digest
  `sha256:0da40f91d79e607afbdad6a41c897878559dbd7a6a3a6348475260aec1409625`；日志无 quota
  refresh/initialization error、panic/fatal/data race/storage error/OOM，PD/TiKV 3+3
  Ready。

- **Operations A377 production quota contract（2026-07-21）**：审计发现 production
  明文/TLS StatefulSet 已部署 quota 指标和四项告警，却没有传
  `--quota-backend-bytes`，标准 dedicated 实例实际处于 unlimited，NOSPACE 与 usage-high
  告警没有保护对象。提交 `90b7240` 为两份基线统一增加 400 GiB
  （429496729600 bytes）logical quota，相对每个 500 GiB TiKV volume 保留容量余量；
  manifest 测试要求两种传输模式都恰好存在一个完全相同的 quota 参数，防止遗漏、重复
  或不同值覆盖。

  生产就绪文档同时固定 logical usage 与 TiKV MVCC/Raft/compaction 物理占用的边界，并
  要求从 unlimited 启用时分两次发布：先让全部副本运行支持共享 dirty tracking、sticky
  NOSPACE 和有界流式 rebuild 的版本并收敛，再统一增加正 quota；回滚也必须整份 Pod
  template 收敛，禁止以部分副本移除 quota 绕过门禁。若首次统计已超限，NOSPACE 是预期
  fail-closed 结果，必须清理数据或提高 quota 后显式 disarm。production manifest 聚焦
  测试 20 轮、完整 `go test ./...` 与 `go vet ./...` 通过；无运行时代码变化，继续使用
  A376 exact image，未另建镜像。

- **Operations A378 quota configuration convergence gate（2026-07-21）**：A377 固定了
  production manifest 的 400 GiB quota，但运行时写门禁仍读取各进程本地
  `QuotaBackendBytes`；原 `KubeBrainQuotaMetricsInconsistent` 只比较 NOSPACE，无法发现
  副本分别使用 100 GiB/400 GiB 等危险漂移。提交 `e1817eb` 要求实例发布门禁显式提供
  正整数 `EXPECTED_QUOTA_BACKEND_BYTES`，并在已确认 StatefulSet image/revision/副本收敛
  后，按 KubeBrain 容器名读取 Pod template args，要求恰好一个完全匹配的 quota 参数；
  缺失、重复、错误值均 fail closed。门禁成功输出同时记录期望 quota，便于控制面审计。

  同轮扩展 quota 一致性告警：NOSPACE、backend quota、logical usage 三类 series 都必须
  恰好三份，并比较三副本 NOSPACE 与 backend quota 值；因此 series 缺失/重复、配置漂移
  或 sticky alarm 未收敛超过一分钟都会告警。首次真实验证发现 kubectl JSONPath 将过滤
  container 与 range args 组合时返回空结果，门禁按设计拒绝；改用按容器名遍历的 Go
  template 后，在当前独立 3 PD/3 TiKV、A376 三副本、1 GiB quota 集群上完整执行
  `validate-instance-ready.sh`，TidbCluster 收敛、精确 image/quota/副本数匹配且 endpoint
  health proposal 成功。错误 quota、重复 quota、必填输入和告警表达式聚焦测试普通 20 轮，
  完整 `go test ./...` 与 `go vet ./...` 通过；无运行时代码变化，未构建新镜像。

- **Compatibility A379 externally reachable MemberList client URLs（2026-07-21）**：
  对照 `/root/etcd` 的独立 `--initial-advertise-peer-urls` 与
  `--advertise-client-urls` 契约，在全新 `a379-full-differential` keyspace 上重跑完整
  Differential 套件。KV/Txn/Watch/Lease/Compact/Hash 等已启用场景通过，但 authenticated
  AutoSync 确定性失败：KubeBrain 从 `--initial-cluster` peer host 推导
  `kubebrain-*.kubebrain-peer...svc:3379`，宿主 clientv3 在 Sync 后用这些仅集群内可解析的
  DNS 整体替换 bootstrap endpoint，随后 Range 因 DNS 不可达超时。完整套件中另有
  leasing range contention 和 paginated mirror 各一次累积负载超时；用新 reference etcd
  隔离重跑各 3 轮后分别在 5.9-7.1 秒、21-39 秒全部通过，未误判为语义差异。

  提交 `601d44d` 新增 `--advertise-client-urls`：一个或多个绝对 `http(s)` URL 必须带
  host/port、不能含 user/path/query/fragment 或规范化后重复，错误配置启动失败。显式值
  经 option、endpoint/server config 传到 MemberList，并覆盖静态成员和动态降级成员的
  ClientURLs；未设置时保留按 peer host、client port 与 TLS 模式推导的旧行为。peer URL、
  leader-election identity 和 follower forwarding 均不改变。production HTTP/TLS 清单
  分别广告同证书身份一致的 client Service 短 DNS；外部 DBaaS 必须覆盖为所有目标客户端
  可路由且匹配证书 SAN 的公共 URL。配置/成员/manifest 聚焦普通 20 轮、race 10 轮、
  完整 `go test ./...` 与 `go vet ./...` 通过。

  exact image `kubebrain:a379-advertise-client-urls` 从
  `601d44d402f86654f040f754acce0260ae75be46` 干净 Git archive 构建，image ID
  `sha256:10dd76444979fa778ecae8c613b077c655d095cc545889c72c885d3a72432283`，OCI
  version `0.0.0-a379.1`、revision 与二进制一致、Go 1.26.5/linux/amd64、TiKV、用户
  `65532:65532`。真实 3 副本滚动后 MemberList 三成员均广告宿主可达
  `http://127.0.0.1:22379`，authenticated AutoSync 对 reference etcd/真实 TiKV-backed
  A379 连续 5 轮全部通过（每轮约 1.5 秒）；Pod UID 为
  `fb0b1afb-5bbb-4c99-9fae-c7eb5a8ac5ac`、
  `8ada1a44-846a-4e02-9945-2cd4a6a9cc94`、
  `e995215f-2a8b-4221-995c-88889e4d8252`，runtime digest
  `sha256:269c01b256fdbed767da86fcc49306a96f9822c023210478abf4614daaa6c7c2`，均 Ready、
  restartCount=0。endpoint health 与 Put/Get/Delete 通过，三副本 quota usage/backend/
  NOSPACE 一致，日志无 panic/fatal/data race/storage/quota/OOM 错误，PD/TiKV 3+3 Ready。

- **Compatibility A380 advertised client URL release gate（2026-07-21）**：
  提交 `ff0e6fc` 将 A379 的 advertised client URL 纳入生产实例发布门禁。调用方必须显式
  提供非空 `EXPECTED_ADVERTISE_CLIENT_URLS`；脚本按 KubeBrain 容器名读取 StatefulSet
  Pod template args，要求恰好一个 `--advertise-client-urls` 且与期望值逐字匹配。参数缺失、
  错误或重复均在 endpoint health 前 fail closed，成功记录同时输出该 URL，便于控制面审计
  外部可达地址和证书身份是否按声明发布。该门禁只验证不可变控制面声明，不把集群内 DNS
  可达性错误地当成外部客户端可达性证明；外部探测仍应使用调用方提供的 `ENDPOINT`。

  门禁专项测试覆盖必填输入以及 advertised client URL 缺失、错误、重复，连续 20 轮通过；
  完整 `go test ./...` 与 `go vet ./...` 通过。在当前 A379、独立 3 PD/3 TiKV、KubeBrain
  三副本集群上，真实执行 `validate-instance-ready.sh`：精确 image、1 GiB quota、
  `http://kubebrain.kubebrain-dev.svc:3379` advertised URL、StatefulSet revision 和全部副本
  均收敛，经过临时本地转发的 endpoint health 成功提交线性化 proposal。验证后已停止端口
  转发；本轮无运行时代码变化，不构建新镜像。

- **Compatibility A381 immutable production keyspace identity（2026-07-21）**：
  审计 A380 发布门禁时发现 image、quota、advertised client URL 和 endpoint health 均可通过，
  但错误或缺省 `--keyspace` 仍会把实例接入另一租户数据边界。提交 `8647e5a` 要求发布调用方
  显式提供非空 `EXPECTED_KEYSPACE`，并要求 KubeBrain Pod template 中恰好一个逐字匹配的
  `--keyspace`；缺失、错误和重复均在健康探测前 fail closed，成功审计记录包含 keyspace。
  production HTTP/TLS 基线同时从隐式默认空间改为显式 `kubebrain-system`；该值仅供静态清单
  基线使用，DBaaS 实例化时必须替换为全局唯一且创建后不可变的稳定实例 ID，不能用可能跨
  集群重复的 namespace 充当全局身份。

  门禁与 manifest 专项测试连续 20 轮通过，覆盖 keyspace 必填输入及参数缺失、错误、重复；
  完整 `go test ./...` 与 `go vet ./...` 通过。在当前独立 3 PD/3 TiKV、A379 KubeBrain
  三副本环境真实执行新门禁，精确匹配 `a379-full-differential` keyspace、A379 image、1 GiB
  quota 和 service advertised URL，StatefulSet 与存储拓扑收敛，endpoint health 成功提交
  线性化 proposal。验证后已停止临时端口转发；本轮无运行时代码变化，不构建新镜像。

- **Compatibility A382 immutable production storage identity（2026-07-21）**：
  继续审计实例发布边界发现，仅锁定 keyspace 仍不能证明实例连接了期望的 TiKV/PD 集群：
  错误 `--pd-addrs` 指向另一个恰好含同名 keyspace 的集群时，endpoint health 仍可能成功。
  提交 `f50f49c` 新增必填 `EXPECTED_PD_ADDRS`，要求 Pod template 中恰好一个逐字匹配的
  `--pd-addrs`；缺失、错误和重复均 fail closed，成功审计记录同时包含存储地址。该检查与
  `TIDB_NAMESPACE`/`TIDB_CLUSTER` 的 Ready、StatefulSet revision 和 3 PD/3 TiKV topology
  收敛组合，控制面必须让期望地址指向同一受管存储集群。

  门禁专项测试连续 20 轮通过，覆盖 PD 地址必填输入及参数缺失、错误、重复；完整
  `go test ./...` 与 `go vet ./...` 通过。当前独立 3 PD/3 TiKV、A379 KubeBrain 三副本
  环境真实执行新门禁，精确匹配 `kb-pd.tidb-cluster.svc:2379`、
  `a379-full-differential` keyspace、A379 image、1 GiB quota 和 service advertised URL，
  endpoint health 成功提交线性化 proposal。验证后已停止临时端口转发；本轮无运行时代码
  变化，不构建新镜像。

- **Compatibility A383 advertised endpoint differential preflight（2026-07-21）**：
  在当前 disposable `a379-full-differential` keyspace 上再次端到端执行完整 reference etcd
  差分。500.773 秒内 Alarm、binary KV、Compact、Delete、Hash/HashKV、Lease/leasing、
  mirror、Put、Range/RangeStream、STM、Txn 和 Watch 通用组均通过；累积负载下最重的
  leasing Put/Get/Delete 并发场景耗时 216.92 秒但完整完成 256 个序列。唯一失败是
  authenticated AutoSync：runner 位于宿主机，在线实例稳定配置广告
  `http://kubebrain.kubebrain-dev.svc:3379`，clientv3 按 MemberList 替换 bootstrap
  端口转发后无法解析集群内 DNS。该结果证明测试网络前提不满足，不是 A379 MemberList
  语义回归；专用 auth/gateway/no-quota 环境的组仍按设计跳过。

  提交 `c4056e5` 让 `run-differential.sh` 在启动 reference etcd 和破坏性测试前读取
  MemberList JSON，提取全部具名非 learner 成员的去重 ClientURLs，要求列表非空并用可配置
  的短超时逐一执行 `etcdctl endpoint health`。任一广告地址对 runner 不可达即 fail closed，
  明确提示从可路由网络运行或发布外部可达 URL；所有 etcdctl 调用统一使用已校验的
  `ETCDCTL_BIN`，TLS 凭据继续沿用标准环境。缺失 URL 与不可达 URL 专项普通 20 轮、race
  10 轮通过；真实在线配置从 bootstrap health 到精确指出 service DNS 不可达约 2.4 秒，
  且未启动 reference etcd。`bash -n`、根模块完整 `go test ./...` 与 `go vet ./...` 通过；
  本机无 shellcheck。本轮只修改测试门禁，不构建数据面镜像。

- **Compatibility A384 clientv3 Barrier and Queue recipes（2026-07-21）**：
  对照 `/root/etcd/tests/integration/clientv3/experimental/recipes/v3_barrier_test.go` 与
  `v3_queue_test.go`，补充此前未进入 DBaaS 黑盒矩阵的高层组合契约。提交 `34fb65b` 使用
  upstream `client/v3/experimental/recipes` 原生实现，同时驱动一次性 reference etcd 和
  真实 TiKV-backed KubeBrain：Barrier 必须拒绝重复 Hold、Release 前阻塞、随后释放全部
  5 个 waiter，且相邻前缀键不能让不存在的精确 barrier 误阻塞；Queue 必须保持五元素
  FIFO，PriorityQueue 按优先级及同优先级 sequence 排序，三 writer/三 reader 并发消费的
  9 个唯一值必须无丢失、无重复。测试既比较双端结果，也固定明确 upstream 期望，避免
  “两端同错”通过。

  真实独立 3 PD/3 TiKV、A379 KubeBrain 三副本环境普通 5 轮和 race 5 轮全部通过，每轮
  约 2.3-3.4 秒；嵌套兼容模块与根模块 `go vet ./...`、根模块完整 `go test ./...` 通过。
  一次性 reference etcd、宿主端口转发、12379/12380/22379 监听和临时 data-dir 均已清理。
  本轮只新增兼容门禁，不修改运行时代码或构建数据面镜像；DoubleBarrier 的 lease/failover
  组合继续作为下一轮 recipe 覆盖。

- **Compatibility A385 clientv3 DoubleBarrier lease recovery（2026-07-21）**：
  对照 `/root/etcd/tests/integration/clientv3/experimental/recipes/v3_double_barrier_test.go`
  和 upstream `double_barrier.go`，提交 `a053616` 增加三参与者双端黑盒差分。前两个 Enter
  必须在 waiter 精确达到 2 后继续阻塞，第三个创建 `/ready` 才释放全部参与者；barrier
  满员后额外 session 必须得到 `recipes.ErrTooManyClients` 且不能留下 ephemeral waiter。
  Leave 阶段前两个参与者必须等待第三个，最终三者全部返回且 waiter 清空。

  独立 failover 场景按 create revision 确定性建立最低序 waiter，三者完成 Enter 后只让
  两个 survivor 执行 Leave，再显式关闭最低序 session 并撤销 lease；两个 survivor 必须
  由 DELETE watch 唤醒且 waiter 全部清空。测试同时固定明确期望和 reference/KubeBrain
  结果，真实独立 3 PD/3 TiKV、A379 三副本环境普通 5 轮及 race 5 轮全部通过，每轮约
  1.6-1.75 秒。嵌套兼容模块与根模块 `go vet ./...`、根模块完整 `go test ./...` 通过；
  一次性 reference etcd、端口转发、监听端口和 data-dir 均已清理。本轮未修改运行时代码
  或构建数据面镜像。

- **Compatibility A386 clientv3 lock ordering recipes（2026-07-21）**：
  对照 `/root/etcd/tests/integration/clientv3/experimental/recipes/v3_lock_test.go` 的
  `TestRWMutex*` 与 `TestMutexWaitsOnCurrentHolder`，提交 `fe1b962` 增加此前基础 Mutex
  handoff 未覆盖的确定性顺序差分。两个先到 reader 必须并发持有 experimental RWMutex；
  writer 创建 waiter 后必须等待全部既有 reader，writer 后到的第三个 reader 又必须等待
  writer，释放顺序固定为 reader→writer→late reader，最终所有 ephemeral key 清空。

  独立 Mutex 场景按 create revision 建立 owner、victim、successor；victim session lease
  撤销并删除中间 waiter 后，successor 仍不得越过当前 owner，只有 owner Unlock 后才能获取
  并清理锁键。测试同时固定明确期望和双端结果，避免随机读写比例掩盖 writer fairness 或
  waiter 越权。真实独立 3 PD/3 TiKV、A379 KubeBrain 三副本环境普通 5 轮与 race 5 轮
  全部通过，每轮约 2.0-2.2 秒；嵌套兼容模块和根模块 `go vet ./...`、根模块完整
  `go test ./...` 通过。一次性 reference etcd、端口转发、监听端口和 data-dir 均已清理；
  本轮未修改运行时代码或构建数据面镜像。

- **Compatibility A387 clientv3 naming manager and resolver（2026-07-21）**：
  对照 `/root/etcd/tests/integration/clientv3/naming/endpoints_test.go` 与
  `resolver_test.go`，提交 `131d8ff` 增加 EndpointManager/gRPC resolver 双端差分。
  EndpointManager 必须把同一 Txn 的两个 Add 作为同 revision 批量 watch 更新，List 保留
  address 与 metadata；单 Txn delete+add 后批量更新和最终列表必须一致，相似的
  `manager`/`manager-other` 前缀严格隔离，附 lease endpoint 在 Revoke 后必须发出 Delete。

  resolver 场景由测试进程启动两个本地 gRPC health server，分别返回 SERVING 与
  NOT_SERVING；通过 etcd naming 注册两地址并建立 `etcd:///` pick_first 连接，识别首次
  实际后端后删除其 EndpointManager 键，resolver 必须消费 prefix watch、移除旧地址并切换
  到另一状态的后端。结果同时与明确期望及 reference etcd 比较。真实独立 3 PD/3 TiKV、
  A379 KubeBrain 三副本环境普通 5 轮及 race 5 轮全部通过，每轮约 0.67-0.74 秒；嵌套
  兼容模块和根模块 `go vet ./...`、根模块完整 `go test ./...` 通过。一次性 reference etcd、
  两个 gRPC server、端口转发、监听端口和 data-dir 均已清理；本轮未修改运行时代码或
  构建数据面镜像。

- **DBaaS A388 cold physical snapshot capability preflight（2026-07-21）**：
  重新核对 KubeBrain transactional TiKV 存储边界与 A143 真实恢复证据，修正文档中把
  TiDB BR full/PITR 写成已完成替代方案的陈旧结论：该路径不包含 KubeBrain transactional
  keys，BR raw 也不提供跨 CF 原子快照。当前仅 `kubebrain.logical.v2` 通过生产恢复门禁；
  物理 full snapshot 和日志型 PITR 均未完成。

  新增 `hack/backup/cold-snapshot-preflight.sh` 作为只读、fail-closed 的候选能力预检。
  调用方必须显式批准 maintenance window，并提供 VolumeSnapshotClass、KubeBrain
  StatefulSet UID、TidbCluster UID、TiKV cluster ID 和预期 PD/TiKV PVC 数量。脚本要求
  VolumeSnapshot API 可用、snapshot class 使用非空 driver 且 `deletionPolicy=Retain`，
  精确核验 3+3 Bound PVC 的 name/UID/PV/storage class/volume mode，并输出规范化
  `kubebrain.cold-physical-snapshot-preflight.v1` inventory。它不停止流量、不暂停 operator、
  不创建快照，也不证明恢复。

  专项普通 20 轮与 race 10 轮通过，覆盖有效 inventory、缺失 API、Delete policy、错误
  KubeBrain/TiDB 身份、未 Bound PVC 和缺少显式批准；`bash -n`、根模块完整
  `go test ./...` 与 `go vet ./...` 通过。对当前真实 3 PD/3 TiKV kind 集群使用在线 UID 和
  cluster ID 执行时，因未安装 `snapshot.storage.k8s.io` API 在任何变更前明确失败关闭。
  因此本轮只建立外部 CSI 依赖和 immutable inventory 契约；下一步仍需在具备 CSI snapshot
  能力的预生产集群实现全停机 fencing/quiesce、原子多 PVC 快照、隔离恢复和数据面验证
  executor，不能把本轮记为 `BACKUP_MODE=cold-csi` 或 PITR 已受支持。

- **Production A389 reachable Go vulnerability release gate（2026-07-21）**：
  增加发布工具链的可达漏洞门禁。使用宿主 Go 1.26.0 执行固定版
  `govulncheck@v1.6.0` 时，真实命中 18 个可达标准库漏洞，覆盖 TLS、x509、HTTP/2、
  template、URL 和 MIME 路径；这些漏洞均已在 Go 1.26.1-1.26.5 修复。重新核验 Docker
  build stage digest `sha256:1ecb7edf62a0408027bd5729dfd6b1b8766e578e8df93995b225dfd0944eb651`
  后确认其实际对应 `golang:1.26.5-bookworm`，
  因此把 Dockerfile 的可读 tag 从浮动 patch 的 `1.26-bookworm` 改为精确
  `1.26.5-bookworm`，digest 保持不变，使人读版本、CI `GO_VERSION` 和构建字节一致。

  CI 新增固定 `golang.org/x/vuln/cmd/govulncheck@v1.6.0` 步骤，分别扫描根模块和
  `hack/backup/objectstore` 独立模块，避免 Go module 边界漏掉生产备份上传代码。使用同一
  Go 1.26.5 digest 容器复扫，两模块均报告 `No vulnerabilities found`；根模块仅存在一个
  未调用的依赖模块漏洞，不属于 symbol-level 可达漏洞。该门禁后续会随漏洞数据库更新而
  fail closed，工具版本升级需作为显式审阅变更。

- **Production A390 Staticcheck gate and dead-code removal（2026-07-21）**：
  使用 Go 1.26.5 与固定 `honnef.co/go/tools/cmd/staticcheck@v0.7.0` 扫描根模块，首次发现
  37 项，包括生产计量、quota alarm、txn compare、watch watermark 的无调用旧 helper，
  已弃用 `ioutil`、Kubernetes backoff 与 gRPC `FailFast` API，以及无效赋值和机械性比较。
  删除确认无调用方的重构遗留代码，迁移为 `io`/`os`、`wait.Interrupted` 和
  `grpc.WaitForReady(true)`，其余使用 `bytes.Equal`/`copy` 等等价实现。根模块和独立
  `hack/backup/objectstore` 模块复扫均为零告警，CI 现对两模块执行固定版 Staticcheck。

  本轮还发现 A389 把 Dockerfile 改为精确 `1.26.5-bookworm` 后，构建契约测试仍硬编码
  `1.26-bookworm`，会令完整 CI 失败；现已让测试同时固定精确 tag 与完整 digest，并把
  in-cluster load smoke 的 Go image 一并固定到相同 tag/digest。根模块完整 `go test ./...`、
  `go vet ./...` 通过；backend、endpoint、全部 server/storage、Prometheus metrics 和全部
  production 包的 race 测试通过，其中 `pkg/server/etcd` race 运行 245.764 秒。

  构建 `kubebrain:a390-staticcheck` 后在真实独立 3 PD/3 TiKV、三副本 StatefulSet 上完成
  滚动更新。首次宿主 load loop 因 `kubectl port-forward service` 绑定的 Pod 被替换而断开，
  rollout 本身成功；随后为三个 Pod 建立独立隧道，三个 endpoint 均可提交 health proposal、
  MemberList 状态指向同一 leader，并逐一从每个副本写入、从全部三个副本线性读取到精确值。
  三 Pod 均为新镜像、Ready、零重启，运行版本 `0.0.0-a390.1`、Go 1.26.5、TiKV；临时隧道
  已关闭。该结果验证 `WaitForReady` 迁移未破坏 follower-to-leader forwarding。

- **Production A391 ShellCheck gate and restore source fencing（2026-07-21）**：
  对全部 72 个 Git 跟踪 shell 脚本运行 upstream 最新稳定 ShellCheck v0.11.0。首次扫描发现
  构建脚本用 `export name=$(command)` 掩盖命令失败、trap 状态未初始化、`BASH_SOURCE`
  数组索引遗漏、局部变量声明掩盖返回码、未引用变量和不安全词拆分等 warning。逐项修复后，
  `bash -n` 与 ShellCheck warning/error 全部通过；jq 单引号程序等 info/style 提示不作为
  发布阻断。CI 使用完整 digest
  `sha256:61862eba1fcf09a484ebcc6feea46f1782532571a34ed51fedf90dd25f925a8d`
  固定 `koalaman/shellcheck:v0.11.0`，并从 `git ls-files '*.sh'` 构造精确输入。开发侧
  addlicense 也从 `@latest` 固定为 v1.2.0。

  扫描同时暴露 post-restore audit 读取冻结 state 的 source instance、source prefix 和 target
  prefix 后未使用。现要求 source instance 非空且与 target 不同，两个 prefix 非空且不同，
  并要求 `kubebrain.restore-cutover.receipt.v1.source_instance` 精确等于 state；任一漂移在持续
  探针开始前 fail closed。新增 source receipt 漂移与相同 prefix 负例。审计脚本专项普通
  20 轮、operation runner 普通 3 轮、build 契约 20 轮及恢复相关 race 通过；根模块完整
  `go test ./...`、`go vet ./...` 通过。TiKV/Badger 两种构建脚本均以完整 SHA/时间/version
  成功产出二进制。本轮不修改数据面存储语义或部署镜像。

- **Compatibility A392 clientv3 balancer under replica shutdown（2026-07-21）**：
  对照 `/root/etcd/tests/integration/clientv3/connectivity/server_shutdown_test.go` 的
  `TestBalancerUnderServerShutdownWatch`、Put/Delete/Txn 和 linearizable/serializable Get
  场景，确认此前测试主要证明服务端恢复或单 endpoint 重试，没有确定性证明已固定 Pod 的
  官方 clientv3 会切换 endpoint。新增 `hack/dev/cmd/balancer-smoke` 和
  `hack/dev/incluster-balancer-smoke.sh`：探针先只连接 victim Pod DNS，完成 unary pin 和
  CreatedNotify prefix Watch，再开放三个 Pod endpoint；双容器 Job 的 fault sidecar 使用
  resourceNames 限定的 namespaced Role，只能 get/delete 该 victim Pod。外层脚本固定 Go
  1.26.5 build digest、从当前 StatefulSet 取得 runtime image、等待 Job Complete/Failed
  condition，并在任一失败时立即输出双容器诊断；Job、SA、Role、RoleBinding 均唯一命名并
  由 trap 清理。`RUN_INCLUSTER_BALANCER_SMOKE=true` 已接入 `hack/dev/verify.sh`。

  在真实独立 3 PD/3 TiKV、三 KubeBrain StatefulSet 上先删除 follower `kubebrain-0`：
  Watch 连续收到 20 个严格递增事件，全部 unary 操作成功，UID 从
  `460c2bf5-547d-48d6-8262-f706ed0150f6` 变为
  `0d2f83d6-b5cc-4740-9191-3cdaad3fe5aa`。随后识别当前 leader 后执行更强故障；首版探针
  立即写入时发现 client 已切到健康 follower，但内部新 leader 尚未就绪，proxy 在 2 秒边界
  返回一次 `Unavailable`。这不是 endpoint balancer 失败，也不能由 clientv3 对不确定写自动
  重放。探针因此改为对首个唯一 key 的条件 Txn 做有界重试和读后消歧，后续 19 次写要求
  直接成功。最终删除 leader `kubebrain-0` 时记录 `transient_failures=1`，20 个 Watch 事件
  无缺失、last revision `467825214963318807`，Put、两类 Get、Txn、Delete 全部恢复，UID 从
  `0d2f83d6-b5cc-4740-9191-3cdaad3fe5aa` 变为
  `2aaea92f-ac4a-43ab-9184-952f3f763923`，三副本 Ready/零重启且无临时 RBAC/Job 残留。
  全仓 `bash -n`、ShellCheck warning/error、根模块完整 `go test ./...`、`go vet ./...` 和
  Staticcheck 均通过。本轮新增兼容故障门禁，不修改数据面运行时代码或镜像。

- **DBaaS A393 cold physical snapshot candidate executor（2026-07-21）**：
  核对 TiDB Operator v1.6.5 源码后确认 `TidbCluster.spec.paused` 只停止成员协调，适合作为
  外部缩容的 operator fence；组件 `suspendAction.suspendStatefulSet` 则会按 TiDB、TiKV、PD
  等顺序前台删除 StatefulSet 并清空 status，不适合保留控制器身份的快照流程。新增
  `hack/backup/cold-snapshot-execute.sh`，消费 A388 不可变 inventory，并在变更前重新运行
  preflight 做规范化全量比对。

  执行器使用 UID/resourceVersion/replica JSON Patch 设置 pause 并停止 KubeBrain、TiKV、PD，
  等待每个 StatefulSet 的 status replicas 归零后逐 PVC 复核 UID，再创建 operation-labeled
  VolumeSnapshot。成功门禁要求全部 snapshot ready，并核验绑定 VolumeSnapshotContent 的
  Retain policy、class、snapshot UID 引用和非空 CSI snapshot handle；随后按 PD、TiKV、解除
  pause、KubeBrain 恢复，最后 fsync+rename 发布 `kubebrain.cold-physical-snapshot.v1`
  receipt。错误 trap 尝试相同恢复，失败绝不发布 receipt，并明确提示 retained partial
  artifacts 需要审计。

  fake-kubectl 状态机专项测试覆盖完整 6 PVC 成功路径和首个 snapshot wait 失败路径，固定
  quiesce/restore 顺序、恢复尝试、receipt 是否发布及 content handle 数量。当前真实 kind
  集群仍缺少 VolumeSnapshot API，必须在任何 pause/scale 前失败；尚无具备 CSI 的隔离恢复
  演练证据，所以该 executor 仍是候选能力，不能关闭 transactional TiKV 物理恢复或 PITR
  缺口，也不能启用 `BACKUP_MODE=cold-csi`。

- **DBaaS A394 cold physical restore blueprint and renderer（2026-07-21）**：
  审计 A393 receipt 后确认 v1 只有 snapshot handle 和扁平 PVC 身份，缺少原 TidbCluster spec、
  PVC access modes/requested storage，以及 snapshot 到源 PVC 的显式映射，无法可靠构造恢复资源。
  因此把新 preflight/receipt 契约升级为 v2，preflight 额外固定去除 status/运行时 metadata 的
  TidbCluster blueprint 和 PVC 恢复字段；executor 只接受完整 v2，并在 receipt 中记录每个
  snapshot 的 source PVC/component。v1 历史制品 fail closed，不用推断默认值伪造可恢复性。

  新增结构化 `hack/backup/cmd/cold-restore-render`。调用方必须显式确认隔离目标，并提供目标
  VolumeSnapshotClass/StorageClass；renderer 验证 source namespace/name/UID/cluster ID、CSI
  driver、Kubernetes DNS 名、PD/TiKV replica 数、PVC/snapshot 一一对应、唯一 handle、volume
  mode/access modes 和 requested storage >= restore size。输出按固定顺序包含预绑定 retained
  VolumeSnapshotContent、VolumeSnapshot、原名 PVC，以及强制 `paused=true` 的原 TidbCluster，
  防止 operator 在恢复卷 Bound 前抢先创建空 StatefulSet。

  单元负例覆盖旧 v1、blueprint 改名、副本/PVC 数不符、重复 PVC 映射、重复 handle 和容量不足；
  A393 fake-kubectl 成功/回滚矩阵同步升级到 v2，并核验源 VolumeSnapshotContent driver 与
  restoreSize。由于 PD 持久数据绑定原 member DNS identity，renderer 禁止重命名语义并要求
  同 namespace/name 的隔离 Kubernetes 集群。当前仍缺目标集群 API/driver/空环境 preflight、
  apply/wait/unpause 状态机，以及真实 CSI 恢复后 cluster ID 和 KubeBrain key/lease/watch 校验，
  所以物理恢复和 PITR 状态保持未完成。

- **DBaaS A395 isolated cold restore target executor（2026-07-21）**：
  新增 `hack/backup/cold-restore-execute.sh`，把 A394 离线 List 推进为 fail-closed 的目标集群状态机。
  调用方必须显式批准 cold restore、指定非空 context，并提供预先核验的 kube-system namespace UID
  与目标 namespace UID。执行器重新运行 renderer 并对 canonical JSON 全量比对，验证目标
  VolumeSnapshot/TidbCluster API、VolumeSnapshotClass driver+Retain、StorageClass provisioner，
  以及 TidbCluster、PD/TiKV StatefulSet、所有 VSC/VS/PVC 名均不存在；因此 manifest 篡改、
  context/namespace 漂移、driver 错配和重复恢复都在 create 前关闭。

  创建预绑定 snapshot 与 PVC 后，状态机等待全部 snapshot ready 和 PVC Bound，核验 TidbCluster
  新 UID 且仍 paused，再以 UID/resourceVersion Patch unpause，等待 Ready 和两个 StatefulSet
  rollout。最终 status.clusterID 必须等于源物理集群 ID。任何 unpause 后的失败都会重新条件 pause，
  并按 TiKV→PD 使用各 StatefulSet UID/resourceVersion/replica test Patch 缩至零；retained VSC、
  VS 和 PVC 不自动删除。成功 receipt 为 `kubebrain.cold-physical-restore.v1`，绑定源 receipt
  SHA-256、目标 Kubernetes/namespace UID、新 TidbCluster UID/cluster ID、全部 VSC handle/UID
  及 PVC/PV inventory。

  fake-kubectl 测试覆盖完整成功、篡改 manifest、目标 TidbCluster 已存在时 create 前失败，以及
  恢复后 cluster ID 错误时的 pause/TiKV/PD 紧急栅栏和无 receipt；Bash、固定 ShellCheck、Go
  测试均通过。当前 kind
  仍无 CSI snapshot API，无法提供真实 handle import/PVC provisioning 证据；executor 也尚未部署
  KubeBrain 验证 revision、key、lease、watch 和写后读，因此本轮不关闭物理恢复或 PITR 缺口。

- **DBaaS A396 cold restore semantic witness chain（2026-07-21）**：
  对照 `/root/etcd/server/lease/lessor.go` 的 lease checkpoint/recover 语义、
  `/root/etcd/server/storage/mvcc/kvstore.go` 的 MVCC restore，以及 clientv3 watch CreatedNotify 行为，
  确认 KubeBrain `pkg/server/etcd/lease.go` 同样持久化 remaining-TTL checkpoint，并在恢复时以
  `now + checkpoint` 重建 deadline；冷停机本身不要求沿用绝对 wall-clock deadline。但此前
  `logical.v2` lease 行只保存导出时 remaining TTL，无法证明物理恢复后的 granted TTL 未漂移。
  现为 lease witness 增加可选 `granted_ttl`，当前 exporter 必须写入；旧 artifact 的逻辑恢复保持
  兼容，物理 verifier 则 fail closed 拒绝缺字段 witness。

  A393 snapshot executor 现强制输入最近生成且非空的 semantic witness，mutation 前通过完整
  logical-status 校验，并在 `kubebrain.cold-physical-snapshot.v2` 中绑定 format、prefix、revision、
  records、leases、artifact 内部 digest 与文件 SHA-256。A394 renderer 同步拒绝没有完整 witness
  binding 的早期 v2 receipt。新增 `hack/backup/cmd/cold-restore-verify` 与 wrapper，先验证
  witness→snapshot→restore receipt hash chain，再在 witness revision 和 current revision 分页读取
  全 prefix，逐键要求 value/create/mod/version/lease ID 完全一致；每个 lease 要求原 ID、原
  granted TTL、正 remaining TTL 和精确 attached keys。随后 CreatedNotify watch 探针执行 leased
  Put、线性 Get、Delete、两次 revision 精确 event 和 Revoke，原子发布
  `kubebrain.cold-physical-semantic-verify.v1`。

  单元测试逐项破坏 value、create/mod revision、version、lease ID、witness bytes 和 receipt SHA
  链并要求失败。真实独立 3PD/3TiKV、三 KubeBrain 副本环境对隔离 prefix 创建 1 permanent key、
  同一显式 600 秒 lease 的 2 keys 后导出；完整链在 revision `467825214963318822` 验证 3 records、
  1 原 lease，watch probe Put/Delete 为 `467825214963318823/467825214963318824`，全部 exact gate
  通过且测试 prefix、lease、port-forward 已清理。该运行证明 verifier 能检测当前真实数据面，
  仍不是 CSI 恢复证据；只有在真正由 A395 恢复的隔离集群重复通过，才能关闭 cold full restore。

- **DBaaS A397 physical witness fail-fast gate（2026-07-21）**：
  对照 `/root/etcd/server/etcdserver/apply/uber_applier_test.go` 的
  `TestUberApplier_Alarm_Quota`，复核 NOSPACE 下 Put、含写 Txn、只读 Txn、Delete、LeaseGrant/
  Revoke 和 Compaction 的允许/拒绝边界；KubeBrain 的 `pkg/server/etcd/quota_test.go` 及
  `hack/etcd-client-compat/quota_alarm_differential_test.go` 已覆盖相同请求矩阵、告警 owner 稳定性和
  跨 endpoint disarm，因此本轮未发现需修改的数据面差异。

  审计同时发现 A396 的旧 `logical.v2` 租约见证虽缺 `granted_ttl`，仍会被 snapshot executor 接受，
  直到恢复后的语义验证才失败。`logical-status` 现提供显式 `REQUIRE_GRANTED_TTL=true` 门禁，完整
  遍历已校验 lease；cold snapshot executor 在任何 pause、缩容或 VolumeSnapshot create 前强制
  启用。无 lease 和当前 exporter 生成的见证继续通过；旧租约 artifact 默认仍可用于逻辑恢复，
  但不能充当物理恢复见证。fake-kubectl 回归测试确认拒绝路径只有只读 preflight 调用，不产生
  Patch、snapshot 或 receipt。

- **DBaaS A459 cold restore manifest receipt binding（2026-07-22）**：
  审计 A395/A396 的 cold restore 链路发现 restore receipt 只绑定 snapshot receipt、target identity、
  PVC 与 VolumeSnapshotContent inventory，未把已审核的 canonical restore manifest digest 写入制品。
  虽然 executor 在 `kubectl create` 前已经重渲染并比较 manifest，但后续 semantic verifier 只能看到
  restore receipt，无法强制旧/弱 receipt 证明实际应用的是该 canonical manifest。

  现 `cold-restore-execute.sh` 在解析 `RESTORE_MANIFEST` 后计算 canonical `jq -cS` SHA-256，并把
  manifest format、item count、VolumeSnapshotContent/VolumeSnapshot/PVC/TidbCluster 计数写入
  `kubebrain.cold-physical-restore.v1`；`cold-restore-verify` 的 receipt chain 校验要求该 manifest
  binding 存在、digest 合法、计数满足 `3*N+1`，且与 restore receipt 中实际 PVC/Content inventory
  数量一致。targeted `TestColdRestoreExecute`、`TestValidateReceiptChain`、完整
  `go test ./hack/backup/... ./hack/production`、`bash -n` 和
  `go vet ./hack/backup/... ./hack/production` 均通过。该项增强物理恢复证据链的可审计性，
  仍不替代真实 CSI 隔离恢复演练，也不关闭 PITR 缺口。
- **DBaaS A460 cold restore semantic manifest verification（2026-07-22）**：
  在 A459 的 restore receipt manifest binding 之上继续收紧最终语义验证：`cold-restore-verify`
  现在要求调用方提供 `RESTORE_MANIFEST_FILE`，读取审核过的恢复 manifest，按 Go canonical JSON
  重新计算 digest，并检查 Kubernetes List、item count、VolumeSnapshotContent/VolumeSnapshot/PVC/
  TidbCluster 计数全部与 restore receipt 的 `restore_manifest` 字段一致；semantic verify receipt
  同时记录 `restore_manifest_sha256`。缺失、篡改或资源计数漂移的 manifest 会在访问恢复 endpoint
  前失败。`go test ./hack/backup/... ./hack/production`、`bash -n` 和
  `go vet ./hack/backup/... ./hack/production` 通过。该项使 final semantic receipt 能直接追溯到
  已审核的 manifest digest，仍不声明 cold full restore 或 PITR 已完成。
- **DBaaS A461 cold restore manifest content binding（2026-07-22）**：
  继续审计 A460 后发现 semantic verifier 虽已读取 manifest 并核对 digest/计数，但尚未独立验证
  manifest 内容是否来自 snapshot receipt。现 verifier 会从 snapshot receipt 读取 source PVC、
  component、snapshot handle、CSI driver、source namespace/TidbCluster UID/cluster ID，并按 renderer
  的 `operation_id + source_pvc` 命名规则重新推导恢复对象名；随后逐项要求
  VolumeSnapshotContent 的 retained deletion policy、driver、snapshotHandle 和 snapshot ref，
  VolumeSnapshot 的 namespace/content ref，PVC 的 source PVC 名、component label 和 dataSource，
  以及 TidbCluster 的 name/namespace/source annotations/paused=true 全部匹配 snapshot receipt。
  即使攻击者同步更新 restore receipt 中的 manifest digest，替换 snapshotHandle、PVC component
  或 TidbCluster 来源身份也会在访问恢复 endpoint 前失败。`go test ./hack/backup/... ./hack/production`、
  `bash -n` 和 `go vet ./hack/backup/... ./hack/production` 通过。该项进一步收紧 cold restore
  语义验证的制品链，仍不关闭真实 CSI 隔离恢复或 PITR 缺口。
- **DBaaS A462 cold restore actual inventory binding（2026-07-22）**：
  继续审计发现 restore receipt 中实际恢复后的 VolumeSnapshotContent/PVC inventory 此前只参与
  数量校验，semantic verifier 未验证这些执行结果是否仍与 snapshot receipt 对应。现 verifier
  强类型读取 restore target 和实际 inventory，要求目标 namespace/TidbCluster/cluster ID 与
  snapshot receipt 一致，VolumeSnapshotContent 的 name、driver、snapshotHandle 和 UID 对应
  source PVC 推导出的恢复对象，PVC 的 name、UID、PV 和 Bound phase 对应 snapshot receipt，
  并拒绝重复 Content、handle、PVC 或 PV。单元负例覆盖 snapshotHandle 漂移、PVC phase 漂移
  和 target cluster ID 漂移；`go test ./hack/backup/... ./hack/production`、`bash -n` 和
  `go vet ./hack/backup/... ./hack/production` 通过。该项把 semantic verify 从“计划制品正确”
  推进到“执行后 inventory 也绑定”，仍不替代真实 CSI 隔离恢复演练。
- **DBaaS A463 cold restore executor inventory gate（2026-07-22）**：
  A462 继续向前审计发现 restore executor 发布 `kubebrain.cold-physical-restore.v1` 前只校验
  实际 PVC/VolumeSnapshotContent 数量与基础字段，若目标集群返回同 operation label 的错误
  Content name、snapshotHandle 或 PVC phase，可能先发布弱 receipt，再依赖最终 semantic verifier
  拒绝。现 executor 从已审核 manifest 生成期望 inventory，在 receipt 发布前要求实际
  VolumeSnapshotContent 的 name/driver/snapshotHandle 精确一致且 UID/handle 唯一，PVC 的 name
  精确一致、UID/PV 非空唯一且 phase 为 Bound；任一漂移都会在未发布 receipt 时触发已有
  unpause 后 emergency fence。fake-kubectl 回归改用 renderer 真实对象名，并新增 Content handle
  漂移与 PVC phase 漂移负例；`go test ./hack/backup/... ./hack/production`、`bash -n` 和
  `go vet ./hack/backup/... ./hack/production` 通过。该项把 inventory 绑定从最终验证前移到
  executor 发布边界，仍不替代真实 CSI 隔离恢复演练。

- **Maintenance A398 persistent CORRUPT alarm gate（2026-07-21）**：
  对照 `/root/etcd/server/etcdserver/apply/uber_applier.go`、`apply/corrupt.go`、
  `apply/uber_applier_test.go:TestUberApplier_Alarm_Corrupt` 及
  `/root/etcd/server/etcdserver/api/etcdhttp/health.go`，修复 CORRUPT mutation 此前返回
  Unimplemented 的公开差异。backend 现以单个 TiKV internal key 保存严格递增、无重复的 member-ID
  集合，activate/deactivate 使用 exact-value CAS 重试，因而不消费用户 revision、不产生 watch event，
  并在并发不同 owner mutation 时不丢更新。Alarm GET/Status 在全部副本读取同一集合；错误 owner
  deactivate 返回成功空列表且不解除全局栅栏。

  有效 CORRUPT 集合会在请求验证和鉴权之后、leader proxy 或存储 mutation 之前，对 Put、
  DeleteRange、含写 Txn、Compact、LeaseGrant 和 LeaseRevoke 返回标准
  `DataLoss: etcdserver: corrupt cluster`；Range/RangeStream 保持可读。该位置同时固定非法
  Put/Delete/Txn 仍优先返回 InvalidArgument，避免 admission interceptor 泄露告警状态或改变错误契约。
  传统 `/health` 返回 503 `ALARM CORRUPT` 并支持 `exclude=CORRUPT`；
  `/readyz/data_corruption` 返回 `alarm activated: CORRUPT`，聚合 readyz 可按检查名排除。

  新增参考 etcd 差分覆盖 activate/get、读可用、全部 mutation 阻断、错误 owner、disarm 和恢复写，
  并新增三 endpoint 用例：Pod 0 激活其真实 member ID，Pod 1 读成功且写 DataLoss，Pod 2 列举并解除，
  Pod 0 恢复写入。32 owner 并发 CAS 在 race 下通过。最终镜像
  `kubebrain:a398-corrupt-alarm-health`（版本 `0.0.0-a398.3`，revision
  `fde09307ba9b7bb35b2f0185e2ae0ade9dc0147c`）在独立 3PD/3TiKV、三 KubeBrain 副本滚动 Ready；
  参考差分、跨 endpoint、完整 compat（109.440 秒）通过，运行时轮询实际观察到 health/readyz
  告警并在解除后恢复，测试 key、alarm、参考进程和 port-forward 均已清理。

- **Maintenance A399 CORRUPT read-only Txn continuity（2026-07-21）**：
  对照 `/root/etcd/server/etcdserver/v3_server.go:Txn` 的 `txn.IsTxnReadonly` 分流，发现
  A398 仅让 serializable read-only Txn 绕过 CORRUPT 栅栏，linearizable read-only Txn
  会错误返回 DataLoss；上游 etcd 则让所有只读 Txn 绕过 Raft CORRUPT applier，并保留原有
  线性读路径。KubeBrain 现以整棵 Txn 请求树判定只读性：空 Txn、linearizable 和
  serializable read-only Txn 在告警期间继续成功；success/failure 任一分支包含 mutation，
  即使运行时未选中该分支，整个 Txn 仍返回 `DataLoss: etcdserver: corrupt cluster`。

  新增公开差分矩阵及 gRPC 回归覆盖上述四类边界，并保留三 endpoint 激活、观察、解除测试。
  聚焦 race、根模块完整测试、vet、staticcheck 均通过；最终镜像
  `kubebrain:a399-corrupt-readonly`（版本 `0.0.0-a399.1`，revision
  `ee3a523843f13a10be24cc73fb5ff988db45fbf1`，本地镜像 SHA256
  `78c58da303b0678597691ba790443007d4c039a45acd21919e221920789418e2`）在独立
  3PD/3TiKV、三 KubeBrain 副本滚动 Ready。上游 etcd 差分和跨 endpoint 测试通过，完整
  compat 108.321 秒通过；最终 alarm 列表为空，`/health` 与 `/readyz` 全部健康，测试进程及
  port-forward 已清理。

- **Maintenance A400 combined alarm transition gate（2026-07-21）**：
  对照 `/root/etcd/server/etcdserver/apply/uber_applier.go:restoreAlarms` 的
  `CorruptApplier -> CappedApplier` 包装顺序，以及 `api/v3alarm/alarms.go` 的幂等
  activate/deactivate，新增 NOSPACE+CORRUPT 公开差分状态机。矩阵覆盖重复激活 NOSPACE、
  同 member 双告警、Alarm(GET NONE)、Status 双错误、Range/只读 Txn 诊断、Put/delete-only
  Txn/LeaseGrant 错误优先级，以及逐项解除。真实上游和 KubeBrain 均证明双告警时 mutation
  返回 CORRUPT DataLoss；解除 CORRUPT 后 Put/LeaseGrant 返回 NOSPACE ResourceExhausted，
  delete-only Txn 恢复可用；解除 NOSPACE 后写入恢复。

  三 Pod 交叉用例分别在 Pod 0/1 激活 NOSPACE/CORRUPT，由 Pod 2 列举并观察 DataLoss，再跨
  Pod 解除并观察错误降级和恢复，证明两个 TiKV 持久集合不是进程本地状态。扩展差分全量运行
  同时发现既有 RangeStream from-key fixture 会扫描其他并行测试的 ASCII key；测试现迁入
  `0xff 0xfe` 高位独占前缀，在保留 `RangeEnd={0}` 语义的同时避免跨用例污染，双端连续
  10 轮通过。提交 `c8e9a6d8c64b45aaa2504a7b50b7772e7b3bb667`；组合差分、三 endpoint、
  聚焦 race、根模块完整测试、根/compat vet 与 staticcheck 均通过，常规完整 compat
  109.754 秒通过。最终 alarm 列表为空，health/readyz 健康，3 KubeBrain、3 PD、3 TiKV
  全部 Ready。因服务端字节未变化，生产运行镜像继续使用已验证的
  `kubebrain:a399-corrupt-readonly`，未制造无意义制品漂移。

- **Maintenance A401 CORRUPT all-replica replacement persistence（2026-07-21）**：
  A398/A400 已证明 TiKV internal key 的跨 endpoint 可见性，但尚不能排除所有 serving process
  重建后告警丢失。新增显式 opt-in 的破坏性门禁：经稳定 NodePort 激活 CORRUPT 并写入诊断 key，
  依次删除 `kubebrain-0/1/2`，每轮等待 Pod UID 变化且 Ready 后重新查询。每次 replacement 后
  Alarm(GET CORRUPT) 均返回原 member owner，Range 保持可读，Put 持续返回 DataLoss；同时
  `/health` 持续 503 `ALARM CORRUPT`，`/readyz/data_corruption` 持续 503
  `alarm activated: CORRUPT`。三个旧进程全部消失后显式 disarm，写入和健康探针恢复。

  门禁普通与 race 模式共运行三轮、累计九次 Pod replacement，分别 24.21、22.75、24.37 秒
  通过；提交 `b3a16e2144125d26a53552cedbe418b850137ca0`。随后完整 compat 107.371 秒、
  根模块完整测试、根/compat vet 与 staticcheck 全部通过；最终 alarm 列表为空，3 KubeBrain、
  3 PD、3 TiKV 全部 Ready。该证据证明 KubeBrain serving process 可无状态替换且告警由共享
  TiKV 持久化，不等价于 PD/TiKV 全集群重启或物理恢复验证。服务端字节未变化，运行镜像继续为
  `kubebrain:a399-corrupt-readonly`。

- **Maintenance A402 CORRUPT full data-plane restart persistence（2026-07-21）**：
  在既有 `TestReplicatedRestartPreservesState` 中加入 CORRUPT 生命周期，使同一 fixture 同时覆盖
  当前值、tombstone 历史、lease attachment、watch replay、binary key 和持久告警。完成全部
  fixture 写入后激活 CORRUPT；重启窗口的并发 Range 允许 Canceled/DeadlineExceeded/Unavailable
  短暂错误但禁止 revision 回退。编排依次替换 3 KubeBrain、3 PD、3 TiKV，并等待每个 Pod Ready
  及最终 TidbCluster Ready=True。重启后 Alarm GET 必须返回原 owner，raw Put 必须 DataLoss，
  health 与 data_corruption readyz 必须保持 503；显式 disarm 后再验证历史、lease、watch replay
  和 revision 单调新写。

  首轮九成员重启已经成功且服务日志返回 DataLoss，但测试用 `status.Code` 直接解析 clientv3 包装
  错误得到 Unknown；修复为 raw gRPC 断言，并将失败清理升级为有界重试、必须成功的 disarm。
  修正后普通与 race 全栈门禁分别 59.55 秒、58.73 秒通过，共再完成 18 次 Pod replacement。
  提交 `b13fa6d68382b6ba5ec8b9ea6deaf8a532b36e19`；随后完整 compat 98.133 秒、根模块
  完整测试、根/compat vet、staticcheck 及脚本语法检查全部通过。最终 alarm 为空，health/readyz
  全绿，3 KubeBrain、3 PD、3 TiKV 均 Ready 且本轮 restartCount=0。该证据关闭 A401 的
  “未覆盖 PD/TiKV 全成员重启”边界，但仍不等价于 VolumeSnapshot/跨集群物理恢复证明。服务端
  字节未变化，运行镜像继续为 `kubebrain:a399-corrupt-readonly`。

- **Maintenance A403 CORRUPT natural lease expiry parity（2026-07-21）**：
  对照 `/root/etcd/server/etcdserver/server.go:revokeExpiredLeases`、
  `v3_server.go:LeaseRevoke` 和 `apply/corrupt.go`，发现上游自然租约过期会经过公开
  LeaseRevoke 及 CORRUPT applier，告警期间保留 lease 和 attached key；KubeBrain 原实现则由
  expiry worker 直接删除 TiKV 数据。旧镜像差分实际观察为 KubeBrain
  `PresentWhileAlarmed=false`、参考 etcd 为 true。expiry worker 现于任何 TiKV mutation 前读取
  持久 CORRUPT 状态，命中时记录 `lease.expire.corrupt_deferred` 并重新调度；disarm 后正常删除
  lease 和 attached keys。

  单元测试以已过期 deadline 固定告警前后状态，聚焦普通/race 均通过；公开差分以 TTL=2 验证
  告警 4 秒后仍保留、解除后最终删除，并在真实上游和 KubeBrain 通过。三副本破坏性门禁进一步
  证明过期 key 穿越 3 个 KubeBrain Pod replacement 后仍存在，普通与 race 分别 24.83 秒、
  25.518 秒通过，disarm 后删除；实现提交 `f1402f65cf6f989426472483e68a5df47c1bdcef`，
  门禁提交 `ab5ed38`。根模块完整测试、根/compat vet 与 staticcheck 均通过，完整 compat
  86.087 秒通过。

  最终镜像 `kubebrain:a403-corrupt-lease-expiry`（版本 `0.0.0-a403.1`，revision
  `f1402f65cf6f989426472483e68a5df47c1bdcef`，BuildTime `2026-07-21T08:18:49Z`，本地镜像
  SHA256 `3628f6308075fb7f194ed160c6acca9f6a2abdd047cbf34e37e3e82478a4900d`）已滚动至三个
  KubeBrain 副本且全部 Ready；3 PD、3 TiKV 同为 Ready。最终 alarm 列表为空，`/health` 和
  详细 `/readyz` 全绿，临时参考 etcd 已停止。

- **Maintenance A404 expired lease signed TTL parity（2026-07-21）**：
  继续对照 `/root/etcd/server/lease/lease.go:Lease.Remaining`、
  `/root/etcd/server/etcdserver/v3_server.go:leaseTimeToLive` 和 A403 的 CORRUPT revoke
  延迟路径，发现上游对仍存在但已过期的 lease 直接返回
  `int64(Remaining().Seconds())`，因而 elapsed whole seconds 为负；KubeBrain 原
  `remainingTTL` 则把所有负值钳制为 0。扩展公开差分后，旧 A403 实际结果为
  `PresentWhileAlarmed=true, NegativeTTL=false, DeletedAfterDisarm=true`，参考 etcd 的
  `NegativeTTL=true`，形成单一可复现差异。实现现保留 Go duration 的有符号向零截断：最后不足
  一秒仍为 0，超过一秒则返回负值；未知/已删除 lease 继续使用 etcd 的 `TTL=-1` 哨兵。

  单元测试同时固定 live sub-second、expired sub-second、elapsed whole-second 和删除失败保留 lease
  的响应；聚焦普通/race、根模块完整测试、根/compat vet 与固定版 staticcheck 全部通过。上游差分
  在新镜像通过；三副本破坏性门禁每轮除持久 alarm、key、health/readyz 外，还要求过期 lease
  TTL 始终为负，普通和 race 分别 27.40 秒、27.891 秒通过，共完成 6 次 Pod replacement；完整
  compat 109.753 秒通过。实现与差分提交
  `7069115fae4f255a2644f97c58df7e3f080cba82`，增强故障门禁提交
  `31e368aafdfdf2746a2c87d4606e591d764e220c`。

  镜像从实现提交的独立干净 worktree 构建，未包含工作区 `go.mod` 改动：
  `kubebrain:a404-corrupt-expired-ttl`（版本 `0.0.0-a404.1`，revision
  `7069115fae4f255a2644f97c58df7e3f080cba82`，BuildTime `2026-07-21T08:37:18Z`，
  本地镜像 SHA256 `a48facf45a3ac57dbe5a0cf4a4df9875ea34785ac030978f83c83372e607f245`）。
  三个 KubeBrain Pod 已滚动至该镜像且 Ready、restartCount=0，3 PD、3 TiKV 同为 Ready；最终
  alarm 为空，`/health` 和详细 `/readyz` 全绿，临时参考 etcd 与构建 worktree 均已清理。

- **Maintenance A405 CORRUPT lease keepalive transition gate（2026-07-21）**：
  对照 `/root/etcd/server/lease/lessor.go:Renew`、
  `/root/etcd/server/etcdserver/api/v3rpc/lease.go:leaseKeepAlive` 及
  `/root/etcd/server/etcdserver/server.go:revokeExpiredLeases`，新增双 lease 公开差分状态机。
  第一条 TTL=3 lease 在 CORRUPT 激活 2 秒后 KeepAlive，必须返回完整 granted TTL=3，并在旧
  deadline 之后仍保留 attached key，证明 renewal 不经过 CORRUPT mutation applier。第二条
  TTL=2 lease 先自然到期：key 因 revoke 被 CORRUPT 阻断而保留，KeepAlive 至少 500ms 不得返回
  或复活 lease；disarm 使 revoke 完成后，同一 stream 必须收到 TTL=0。

  真实上游 etcd 与 A404 KubeBrain 首轮完全一致，串行重复 3 轮 44.636 秒、race 15.684 秒通过；
  完整 compat 94.624 秒、compat vet 与固定版 staticcheck 全部通过。一次将 repeat 与 race 同时
  指向同一对破坏性端点的验证产生交叉 alarm，LeaseGrant 正确返回 DataLoss；改为串行编排后稳定
  通过，因此此类状态机不得并行共享实例。门禁提交
  `ef3f4bb97a8154464f95e354b173f751f4533962`。本轮未发现服务端差异，不重新构建镜像；运行态继续
  使用 `kubebrain:a404-corrupt-expired-ttl`，最终 alarm 为空，health/readyz 全绿，3 KubeBrain、
  3 PD、3 TiKV 均 Ready，临时参考 etcd 已停止。

- **Maintenance A406 NOSPACE lease recovery gate（2026-07-21）**：
  对照 `/root/etcd/server/etcdserver/apply/capped.go`、
  `/root/etcd/server/etcdserver/apply/uber_applier_test.go:TestUberApplier_Alarm_Quota` 和
  lessor expiry/renew 路径，新增三 lease 公开差分状态机。NOSPACE 激活后新 LeaseGrant 必须返回
  ResourceExhausted；TTL=30 lease 的显式 Revoke 必须成功并删除 attached key；TTL=2 lease 必须
  自然过期并删除 key；TTL=3 lease 在 1.5 秒时 KeepAlive 必须返回完整 TTL=3，且在原 deadline
  之后仍存活。即使两条删除路径已释放逻辑空间，NOSPACE 仍须 sticky，只有显式 disarm 才解除。

  真实上游 etcd 与 A404 KubeBrain 首轮一致；串行重复 3 轮 25.684 秒、race 9.581 秒通过。根模块
  完整测试、完整 compat 91.996 秒、根/compat vet 与固定版 staticcheck 全部通过。门禁提交
  `cb3e9c180e5f4ac4814dc7b54d2a5cd67a71625c`。本轮未发现服务端差异，不重新构建镜像；运行态
  继续使用 `kubebrain:a404-corrupt-expired-ttl`。最终 alarm 为空，health/readyz 全绿，
  3 KubeBrain、3 PD、3 TiKV 均 Ready，临时参考 etcd 已停止。

- **Maintenance A407 explicit unsupported-surface gate（2026-07-21）**：
  复扫 etcd API descriptor 的 42 个公开 RPC、KubeBrain production receiver 和矩阵分类，确认没有
  依赖 `Unimplemented*Server` 嵌入而意外暴露的缺口。新增 AST 门禁进一步限定公开 handler 中
  `codes.Unimplemented` 的唯一允许集合：上游同样条件拒绝 custom-sort/revision-filter 的
  RangeStream、非法 Alarm 类型，以及由 DBaaS 控制面替代的 MemberAdd/Remove/Update/Promote、
  Snapshot、MoveLeader、Downgrade。任何其他已兼容 RPC 将来被显式降级都会使测试失败；提交
  `19ec951`。聚焦普通/race、根模块完整测试、root vet 与固定版 staticcheck 全部通过。

  首轮完整 compat 在 lease generation 线性化基线记录到 1 个模糊 RPC 结果，但旧断言只保留计数，
  无法区分连接瞬断、限流或服务端状态。聚焦连续 10 轮未复现；门禁现保持“基线必须零模糊失败”
  的严格判定，同时在失败信息中保留 client、operation、输入和原始 RPC error，提交 `0e67336`。
  增强后聚焦普通 3 轮、race 及完整 compat 121.074 秒通过，compat vet/staticcheck 全绿，因此当前
  没有证据支持放宽判定或修改服务端。服务端字节未变化，运行镜像继续为
  `kubebrain:a404-corrupt-expired-ttl`；最终 alarm 为空，health/readyz 全绿，3 KubeBrain、
  3 PD、3 TiKV 均 Ready。

- **Maintenance A408 lease workload isolation gate（2026-07-21）**：
  对 A407 首轮 lease generation 基线中的单次模糊 RPC 做运行态归因。失败窗口内 leader 未切换，
  三副本没有 fencing、Unavailable 或健康异常，但日志同时存在 64 个不属于该 generation 历史的旧
  lease KeepAlive；后续检查确认没有残留测试进程，旧流停止后 lease list 为空。为区分正常续约压力与
  环境遗留负载，在 64 条并发 client/v3 KeepAlive 流持续期间将 generation 线性化历史连续运行 10 轮，
  全部通过且没有模糊 RPC；renewal soak 37.459 秒通过并完成清理。因此没有证据支持放宽 A407 的零
  模糊失败基线，也没有证据要求服务端改动。

  renewal soak 的退出门禁进一步逐个检查 64 个已撤销 lease 的 TimeToLive 必须为 -1，并继续要求所有
  attached keys 为空，防止成功测试在共享集群留下续约负载。服务端字节与运行镜像不变，继续使用
  `kubebrain:a404-corrupt-expired-ttl`。

- **Maintenance A409 explicit revoke list-visibility gate（2026-07-21）**：
  对照 `/root/etcd/server/etcdserver/v3_server.go:LeaseLeases` 与
  `/root/etcd/server/lease/lessor.go:Leases`，补齐显式 LeaseRevoke 的完成边界。双端公开差分现要求
  Revoke 返回后 attached key 已删除、TimeToLive 为 -1，且紧随其后的 LeaseLeases 不再包含该 ID；
  64-lease renewal soak 清理也在逐租约 TTL=-1 之外，用一次列表快照拒绝任何本轮 ID 残留。

  参考 etcd 与真实 TiKV-backed KubeBrain 串行差分连续 10 轮通过，64 条 KeepAlive soak 普通
  39.962 秒、race 39.605 秒通过，完整 compat 104.862 秒、compat vet 与固定版 staticcheck 全绿。
  一次将 revision 敏感的双端差分和仅施加于 KubeBrain 的 soak 并发运行，按预期造成相对 revision
  不同；隔离背景写后全部通过，新增的 `ListedAfterRevoke=false` 在污染运行中也始终一致。该结果同时
  固定测试编排约束：比较 revision 的差分不得承受不对称写负载。未发现服务端差异，不重建镜像；
  最终 lease list 与 alarm 均为空，endpoint health 正常。

- **Maintenance A410 direct-replica lease-list authority gate（2026-07-21）**：
  对照 `/root/etcd/server/etcdserver/v3_server.go:LeaseLeases` 和 lessor 的 leader 权威状态，扩展
  三副本直连门禁。通过三个独立 Pod endpoint 逐一验证：Grant 并绑定 key 后，Range、带 keys 的
  TimeToLive 和 LeaseLeases 都能观察同一 live lease；Service endpoint Revoke 返回后，先以每个
  endpoint 的线性 Range 建立观察屏障，再要求 TimeToLive=-1、keys 为空且 LeaseLeases 不含该 ID。
  这防止 follower 将本地启动快照用于列表响应，而绕过现有 TTL 转发测试。

  真实 3 KubeBrain、3 PD、3 TiKV 上聚焦连续 10 轮 2.298 秒、race 3 轮 1.983 秒通过；设置全部
  三个直连 endpoint 后完整 compat 102.029 秒通过。根模块完整测试、root/compat vet 与固定版
  staticcheck 全绿。未发现服务端差异，不重建镜像。

- **Maintenance A411 watch validation-precedence parity（2026-07-21）**：
  对照 `/root/etcd/server/storage/mvcc/watcher.go:watchStream.Watch`，新增同时携带重复 watch ID
  与 `key==range_end` 的公开双端组合输入。上游先校验范围，返回
  `mvcc: watcher range is empty`；KubeBrain 过去先检查已占用 ID，错误返回
  `mvcc: duplicate watch ID provided on the WatchStream`。现将空/逆序范围校验移到 duplicate-ID
  检查之前，仍在分配 logical watch quota、watch ID 和 backend subscription 之前失败；同一 stream
  随后的合法 duplicate、create 和 cancel 行为保持不变。确定性单测同时固定“组合非法先 range、
  单独重复再 duplicate”的顺序。

  修复前真实双端差分稳定复现，部署修复镜像后连续 10 轮通过；聚焦普通 20 轮、race 10 轮、根模块
  完整测试、完整 compat 113.419 秒、root/compat vet 与固定版 staticcheck 全绿。生产镜像
  `kubebrain:a411-watch-validation` 的本地 digest 为
  `sha256:4117d11834bd7bc77237777c9f80c4a8451bcf2b23ad1fbad09a77383835abac`，OCI revision 与容器内
  `kube-brain version` 均为 `5bd6f86fbbd10e52b9ccbc6896ecfd9b4213e061`；三副本已滚动收敛。

- **Maintenance A412 watch validation-precedence chain gate（2026-07-21）**：
  继续对照 `/root/etcd/server/etcdserver/api/v3rpc/watch.go` 与
  `/root/etcd/server/storage/mvcc/watcher.go`，固定 create 请求的完整组合顺序。先占用显式
  `math.MaxInt64` watch ID，再发送同时具备同一重复 ID、`key==range_end` 和负 start revision 的
  请求；参考 etcd 与 KubeBrain 都必须优先返回
  `etcdserver: mvcc: required revision has been compacted`。去掉负 revision 后，同一组合必须返回
  empty-range；再去掉非法范围后才返回 duplicate-ID。完整 RPCServer 单测同时证明三重非法响应不会
  关闭 multiplexed stream，后续合法 watch 仍可创建。

  真实双端差分连续 10 轮、确定性普通 20 轮、race 5 轮、根模块完整测试、完整 compat 111.795 秒、
  root/compat vet 与固定版 staticcheck 全绿。A411 服务端实现已经符合该顺序，本轮只增强回归门禁，
  运行镜像继续使用 `kubebrain:a411-watch-validation`。

- **Maintenance A413 follower-local watch rejection parity（2026-07-21）**：
  对照 `/root/etcd/server/etcdserver/api/v3rpc/watch.go:recvLoop`，修复 follower 在处理可本地拒绝的
  WatchCreateRequest 前过早调用 leader `SyncReadRevision` 的差异。旧实现遇到 leader read barrier
  故障时，会把负 start revision 或非法范围升级为 stream 级 `Unavailable`；上游分别直接返回
  compacted 或 empty-range canceled response，且 stream 保持可用。现按
  `normalize -> negative -> auth -> range -> read barrier -> duplicate/quota/backend` 执行：本地确定性
  错误不依赖 leader，可能真正创建 watch 的请求仍必须取得同步 revision fence，不能降级为 stale watch。

  注入测试让 follower barrier 固定失败，证明前两条本地非法请求仍返回 control response，第三条合法
  create 只调用一次 barrier 并以 Unavailable fail closed；聚焦普通 20 轮、race 5 轮通过。新增三个
  Pod 直连黑盒门禁，逐副本验证两条本地拒绝后仍能创建/取消合法 watch，连续 10 轮及 race 5 轮通过；
  启用直连门禁的完整 compat 126.353 秒、根模块完整测试、root/compat vet 与固定版 staticcheck 全绿。

  镜像 `kubebrain:a413-watch-local-reject` 的本地 digest 为
  `sha256:5e1f1ca1d5e949eb39fcb10410fe6c7f93dd7b30362a38ed288320f01cef5eed`，OCI revision 与容器内
  `kube-brain version` 均为 `7083108090fc0145969ac6e65664f3e3ef210b25`；三副本已滚动收敛。

- **Maintenance A414 follower-local duplicate watch rejection parity（2026-07-21）**：
  继续对照 `/root/etcd/server/storage/mvcc/watcher.go:watchStream.Watch`。上游在 stream-local
  watcher map 中识别显式重复 ID，并直接返回 `ErrWatcherDuplicateID`，不依赖集群 revision read
  barrier。KubeBrain 在 A413 后仍先执行 follower `SyncReadRevision`，因此 leader barrier 故障会把
  本可确定返回的 duplicate canceled response 错误升级为 stream 级 `Unavailable`。现将显式非零
  watch ID 的 stream-local 预检查移到 read barrier 之前，同时保留 `watcher.start` 内的加锁复检，
  避免并发状态变化绕过最终注册保护；合法新 ID 仍必须通过 barrier，不能创建 stale watch。

  注入测试先成功注册 ID 414，再令 follower barrier 失败：重复 ID 414 必须不新增 barrier 调用并
  返回 `mvcc: duplicate watch ID provided on the WatchStream`，新 ID 415 则执行 barrier 并 fail
  closed；相关 follower barrier 测试普通 20 轮、race 5 轮通过。三 Pod 直连黑盒门禁扩展为逐副本
  验证 negative、empty-range、duplicate、本地 stream 复用和合法 cancel，普通 10 轮、race 5 轮及
  环境重启后的复验均通过；启用直连门禁的完整 compat 114.164 秒、稳定环境上的根模块完整测试、
  root/compat vet 与固定版 staticcheck v0.7.0 全绿。

  镜像 `kubebrain:a414-watch-duplicate-local` 的本地 digest 为
  `sha256:60c2655adf2244b18926627553501c1307706273ef7634b75f082942f9079651`，OCI revision 与容器内
  `kube-brain version` 均为 `32daaca971b3ff0731bd9d4ed3a6e0bb1fec05d2`，容器继续以
  `65532:65532` 非 root 身份运行；三副本已滚动收敛。

- **Maintenance A415 follower-local watch cancel parity（2026-07-21）**：
  对照 `/root/etcd/server/etcdserver/api/v3rpc/watch.go:serverWatchStream.recvLoop`，修复 follower
  在处理 WatchCancelRequest 前错误执行 leader `SyncReadRevision` 的差异。上游 cancel 只删除当前
  stream 的本地 watch、取消其 backend context 并返回 canceled response；它不是线性化读。旧实现
  在 leader barrier 故障时会先返回 stream 级 `Unavailable`，导致请求中的 logical watch 未被单独
  cancel，multiplex stream 上其他 watch 也被连带关闭。现取消路径不再发起 barrier，response header
  使用 stream 已同步的 control revision 与本地 published revision 的较大值；后续新 create 仍必须
  通过 barrier，不能借此绕过 revision fence。

  修复前注入用例稳定只收到 created response；修复后同一 follower proxy stream 先以 revision 60
  创建 ID 415，在下一次 barrier 固定失败时仍本地 cancel、返回 revision 60 且立即取消代理 context，
  随后的 ID 416 create 才执行第二次 barrier 并 fail closed。相关 follower barrier 测试普通 20 轮、
  race 5 轮通过；三 Pod 直连门禁普通 10 轮、race 5 轮通过，启用直连门禁的完整 compat 93.009 秒，
  根模块完整测试、root/compat vet 与固定版 staticcheck v0.7.0 全绿。

  镜像 `kubebrain:a415-watch-cancel-local` 的本地 digest 为
  `sha256:6478d4d2987df032758aa8cda3c2811768441663b6e3d54d5122fbc98dab3d0d`，OCI revision 与容器内
  `kube-brain version` 均为 `2af56ef869511cde6f1809add804479340f8b80f`，运行用户为
  `65532:65532`；三副本已滚动收敛。

- **Maintenance A416 follower-local watch quota admission（2026-07-21）**：
  延续 A413-A415 的 follower-local control response 审计，修复 logical Watch 配额已满时仍先执行
  leader `SyncReadRevision` 的故障耦合。`--max-watches` 是当前 KubeBrain 进程的资源保护；当本地
  active counter 已达上限，结果完全由本地状态决定，不应因 leader barrier 故障变成 stream 级
  `Unavailable`。现于 barrier 前原子 acquire 槽位：配额已满直接返回 `WatchId=-1`、
  `etcdserver: too many requests` 的 canceled-create；成功预留后才进入 revision fence，并在 barrier
  失败、forward auth 失败或 proxy 禁用等所有注册前出口释放槽位，成功注册后则把所有权移交给
  logical watch，由 cancel、backend close 或 stream close 释放。未启用配额时不增加计数。

  修复前故障注入稳定返回 `Unavailable` 且调用一次 barrier；修复后 quota-full 不调用 barrier，
  multiplex stream 保持协议级可用。额外用例证明 barrier 失败和 barrier 成功但 proxy 禁用均不会
  泄漏预留；配额、并发原子上限、cancel/disconnect 相关测试普通 20 轮与 race 5 轮通过。真实集群
  临时设置 `--max-watches=1`，固定当前 leader 的 raw gRPC 占满/拒绝/cancel/复用状态机连续 20 轮
  通过，随后恢复生产值 10000。固定 follower 连续测试同时确认该限制仍是 per-process：proxy watch
  会分别占用 ingress 与 leader 的资源槽位，leader 侧异步关闭窗口可独立返回 quota rejection；本轮
  不把它误述为跨副本的分布式租户总量配额。

  三 Pod 常规直连门禁普通 10 轮、race 5 轮通过；启用直连门禁的完整 compat 88.063 秒，根模块
  完整测试、root/compat vet 与固定版 staticcheck v0.7.0 全绿。镜像
  `kubebrain:a416-watch-quota-local` 的本地 digest 为
  `sha256:6761159336232b7eb1499aacab2c499587b9b256e7b261c6115993dbe1a4bd5d`，OCI revision 与容器内
  `kube-brain version` 均为 `f2212990b5521a94d51e0246fe9222b3ff71abb8`，运行用户为
  `65532:65532`；三副本已恢复 `--max-watches=10000` 并滚动收敛。

#### A417 LeaseKeepAlive 阻塞接收取消与 gRPC 状态归一化

- 对照 `/root/etcd/server/etcdserver/api/v3rpc/lease.go`，上游 `LeaseKeepAlive` 在独立接收
  goroutine 外同时等待 `stream.Context().Done()`；KubeBrain 此前直接阻塞于 `Recv()`，handler
  的取消退出依赖具体 gRPC transport 唤醒接收。现拆分 `leaseKeepAlive` 接收循环，并由公开 RPC
  handler 仲裁接收结果与 stream context；错误通道容量为 1，取消竞争不会阻塞接收 goroutine
  收尾。context 退出通过 `status.FromContextError` 转为标准 gRPC `Canceled`，避免 Prometheus
  将标准库 `context.Canceled` 记为 `Unknown`。
- 新增可控阻塞 `Recv` 的确定性回归，证明 context 取消不等待 transport 接收返回；同时把过期
  lease 等待和 follower proxy 取消用例收紧为 `codes.Canceled`。聚焦普通 20 轮、race 5 轮、
  `pkg/server/etcd` 全包通过。首次真实 follower 指标门禁准确发现 1000 次取消被归类为
  `Unknown`，完成状态归一化后连续 5 轮通过，共覆盖 1000 次建流、发送和取消，`Canceled`
  增长且 `Unavailable` 不增长。
- 显式设置当前 NodePort 后，独立 compat module uncached 全量 117.702 秒通过，root/compat vet
  与 staticcheck v0.7.0 通过。镜像 `kubebrain:a417-lease-stream-cancel-local` digest 为
  `sha256:a1d8e8ae831d53914eb99c176a010e0dbb5b0bbafb04867c3568f75a05b67615`，OCI revision 与
  容器版本为 `b8d216a1b00a78ff59692ad946c0176ddb3f3e16`，运行用户 `65532:65532`；三副本滚动
  收敛后 Ready、零重启，主 PD/TiKV 3+3 Ready，health/readyz 正常且无 alarm。
- 本轮还复核了两个候选项但未伪造缺口：raw gRPC Watch `CloseSend` 后 response side 保持存活
  与 reference etcd 一致；`LeaseCheckpointRequest` 只属于 `InternalRaftRequest`，不在公开
  `LeaseServer` RPC 面。公开 42 个 RPC 的显式实现/拒绝门禁继续覆盖 API 升级。

#### A418 Watch stream 取消状态归一化

- 对照 `/root/etcd/server/etcdserver/api/v3rpc/watch.go`，上游 Watch handler 在 receive loop
  或 stream context 返回 `context.Canceled` 时统一转换为
  `rpctypes.ErrGRPCWatchCanceled`。KubeBrain 此前在 raw gRPC `CloseSend` 后等待 context 的
  路径及若干内部取消路径直接返回标准库错误，gRPC Prometheus 会将其记为 `Unknown`，而非
  etcd 客户端和 SLO 预期的 `Canceled`。现于 `RPCServer.Watch` 单一出口检查错误链，只归一化
  `context.Canceled`，不改变 compaction、auth、quota、read barrier 或 transport 错误。
- 现有负 revision、follower revision fence、quota、HTTP half-close、auth 和 forwarded watch
  测试均改为约束公开 `codes.Canceled`；聚焦普通 20 轮、race 5 轮及 `pkg/server/etcd` 全包
  通过。新增真实指标门禁每轮必须先收到 watch-created，再取消 stream；固定真实 follower
  连续 5 轮共 1000 次创建/取消全部通过，`grpc_server_handled_total{grpc_code="Canceled"}`
  增长，`Unknown` 与 `Unavailable` 均不增长，逻辑 watch 正常释放。
- root 全量测试/vet/staticcheck v0.7.0 通过，独立 compat module 指定真实 NodePort 后 uncached
  全量 105.011 秒通过。镜像 `kubebrain:a418-watch-cancel-status-local` digest 为
  `sha256:ef84a2893520a222b87f8969bf2acc4aa3d4492c780dfba66eb9287eb98a6f8a`，OCI revision 与
  容器版本均为 `1e20dad823744ad97180ddff1c2883da3e60c87a`，运行用户 `65532:65532`；三
  KubeBrain 副本 Ready、零重启，主 PD/TiKV 3+3 Ready，health/readyz 正常且无 alarm。
- 本轮候选审计同时确认：KV 非法 sort/空 operation/嵌套 txn budget 已有差分矩阵；Lease TTL
  边界已有上游 `MaxLeaseTTL` 对齐；上游 gRPC Health 的 `allGRPCServices` 即空字符串，当前
  service-name 注册不存在缺口。未把这些已对齐路径重复包装成修复。

#### A419 Watch fragmentation protobuf 字段升级门禁

- 对照 `/root/etcd/server/etcdserver/api/v3rpc/watch_test.go` 的
  `TestWatchResponseProtoFieldCount`，补齐 KubeBrain 自有 `sendWatchFragments` 手工字段复制路径
  的同等结构门禁。当前 `WatchResponse` 的 8 个 protobuf 字段为 Header、WatchId、Created、
  Canceled、CompactRevision、CancelReason、Fragment 和 Events；未来 API 依赖新增字段时测试会
  立即失败，要求同步更新分片复制，避免大 Watch response 才触发的静默字段丢失。
- 现有 fragmentation 测试同时扩展为所有非 Events 字段逐片 wire 语义保真，并继续验证事件
  不重不漏及仅末片 `Fragment=false`。protobuf message 使用 `proto.Equal` 比较，避免依赖
  `sizeCache` 等运行时内部状态。聚焦普通 50 轮、race 10 轮及 `pkg/server/etcd` 全包通过。
- 本轮审计确认当前实现没有运行时字段遗漏，只新增永久 API 升级门禁；生产服务端字节不变，
  因此不重建与 A418 相同的镜像，运行态继续使用已验证的
  `kubebrain:a418-watch-cancel-status-local`。

#### A420 RangeStream 拆分响应 protobuf 字段升级门禁

- 对照 `/root/etcd/server/etcdserver/v3_server.go` 的 RangeStream 合并契约，审计 KubeBrain
  自有 `splitRangeStreamResponse` 二次按 wire size 拆分路径。当前 `RangeResponse` 的 Header、
  Kvs、More、Count 四个 protobuf 字段均完整处理，外层 `RangeStreamResponse` 当前仅含
  RangeResponse；新增两类结构字段计数门禁，未来依赖升级新增任一字段时会立即要求同步更新
  手工拆分逻辑，避免仅在大响应中出现静默字段丢失。
- 新增完整语义测试，以包含四项 Header 标识、三项 KV、More 和 Count 的响应强制触发多段
  拆分；约束非末段不携带终态元数据，并验证所有分段经 `proto.Merge` 后与源响应完全一致。
  聚焦普通 50 轮、race 10 轮、`pkg/server/etcd` 全包以及 root 全量测试、vet、staticcheck
  v0.7.0 均通过。
- 本轮确认当前生产实现没有字段遗漏，仅新增 API 升级回归门禁；生产服务端字节不变，因此
  不重建与 A418 相同的镜像，运行态继续使用已验证的
  `kubebrain:a418-watch-cancel-status-local`。

#### A421 RangeStream context 结束状态归一化

- 对照 `/root/etcd/server/etcdserver/api/v3rpc/key.go` 与 `util.go`：上游 RangeStream 经
  `togRPCError` 保留 `context.Canceled`/`DeadlineExceeded`，由 gRPC transport 转换客户端
  code。KubeBrain 同样保留 raw context error 时，Prometheus stream interceptor 会在
  transport 转换前读取 handler 返回值，导致一部分已取消请求记为 `Unknown`。现于
  `RPCServer.RangeStream` 单一出口使用 `status.FromContextError` 显式归一化两类 context
  结束状态；客户端公开 code 不变，服务端 SLO 指标稳定为 Canceled/DeadlineExceeded。
- 单元测试在 backend scanner channel 已建立后分别触发 cancel 与 deadline，聚焦普通 50 轮、
  race 10 轮及 `pkg/server/etcd` 全包通过。真实旧 A418 同 Pod 双端口验证 200 次立即取消时
  `Unknown` 从 0 增至 59，证明原问题；最终门禁先写入 64 个 64 KiB value、收到首 chunk
  确认 handler 已进入后再取消，A421 连续 5 轮共 500 次全部通过，每轮 Canceled 增长且
  Unknown/Unavailable 均不增长。独立 compat module 指定真实 NodePort 后 uncached 全量
  95.089 秒通过；root 全量测试、vet、staticcheck v0.7.0 均通过。
- 镜像 `kubebrain:a421-range-stream-context-status-local` 从 `git archive` 的已提交树构建，
  本地 image ID 为 `sha256:43cd6345b08d8e2b085f8707d2553e09a79389d00e6593f80aa8a09e59b98266`，
  kind/containerd 导入后的运行 digest 为
  `sha256:355341f306ca5ed477c4ba0439dab371af83fb86d382ef3e2b634b83fca543b0`；
  OCI revision 与容器版本均为 `52cd51dff000da1befa7d0f4a658fa363ae42105`，运行用户
  `65532:65532`。三 KubeBrain 副本 Ready、零重启，主 PD/TiKV 3+3 Ready，health/readyz
  正常且无 alarm。真实门禁的时序强化另以测试提交记录，不改变该生产二进制。

#### A422 gRPC 全局 context 状态归一化

- A421 的 handler 内归一化仍无法覆盖 generated gRPC handler 在读取首条请求前发生的取消：
  该路径不会进入 `RPCServer.RangeStream`，最外层 Prometheus interceptor 仍可能先看到 raw
  `context.Canceled` 并记为 Unknown。现于 endpoint 的 client 与 peer server option 链中，
  在最外层 metrics interceptor 和内层 admission/stamping/generated handler 之间统一插入
  unary/stream context status normalizer；所有下游 `context.Canceled` 和
  `DeadlineExceeded` 经 `status.FromContextError` 转成标准 gRPC status，其他错误原样保留。
  A421 的 RangeStream 局部 defer 及其直调测试已删除，状态归一化职责收敛到唯一边界。
- 新增 interceptor 顺序集成测试，覆盖 client/peer × unary/stream × canceled/deadline，并继续
  约束 metrics 必须观察 handler 前 ResourceExhausted，证明新层没有改变 admission 错误码或
  把 metrics 移到内层；聚焦普通 50 轮、race 20 轮及 endpoint/server 包通过。真实同 Pod
  双端口门禁连续 5 轮：收到首 RangeStream chunk 后取消共 500 次，请求创建后立即取消共
  1000 次；每轮 Canceled 增长，Unknown/Unavailable 均不增长。真实 NodePort 的独立 compat
  module uncached 全量 102.094 秒通过；root 全量测试、vet、staticcheck v0.7.0 均通过。
- 镜像 `kubebrain:a422-grpc-context-status-local` 从 `git archive` 提交树构建，本地 image ID
  为 `sha256:86da7520c4ebee1e612ada0156088ec96ae42d38b2de3bcf89c4256b632a3ad4`，
  kind/containerd 运行 digest 为
  `sha256:294b0941df69041ac93c8957c07dbcad4bf024423947d4dfe2daff68b44dd79f`；
  OCI revision 与运行版本均为 `8544e95880f4759c10e273a8b3e781633f14fda6`，Go 1.26.5、
  TiKV backend、用户 `65532:65532`。三 KubeBrain 副本 Ready、零重启，主 PD/TiKV 3+3
  Ready，health/readyz 正常且无 alarm。

#### A423 Watch Event PrevKv 过滤字段升级门禁

- 审计 `withoutWatchPrevKvs` 的手工 Event 浅复制路径：同一 backend event 会扇出给同时请求和
  不请求 PrevKv 的 watcher，因此不能原地清空；使用 `proto.Clone` 又会深复制可能很大的 KV
  Value，放大 Watch 热路径 CPU 与内存。当前 `mvccpb.Event` 仅有 Type、Kv、PrevKv 三个
  protobuf 字段，现有浅复制没有字段遗漏；新增三字段结构门禁，未来 etcd API 增加 Event
  字段时测试立即要求更新复制逻辑，避免仅 `PrevKv=false` watcher 静默丢字段。
- 既有不修改共享 event 测试扩展为完整约束：PUT/DELETE Type 保真、Kv 指针保持零拷贝、
  输出 Event 对象与源隔离、nil event 保留、输出 PrevKv 清空且源 PrevKv 不变。聚焦普通
  100 轮、race 20 轮、`pkg/server/etcd` 全包以及 root 全量测试、vet、staticcheck v0.7.0
  均通过。
- 本轮确认当前生产实现无字段遗漏，只新增 API 升级失败快显门禁；服务端字节不变，不重建
  与 A422 相同的镜像，运行态继续使用已验证的
  `kubebrain:a422-grpc-context-status-local`。

#### A424 backend watch KeyValue 转换字段升级门禁

- 审计 `watchTranslator.kvToEtcdKv` 及 DELETE tombstone 构造：当前 `mvccpb.KeyValue` 的
  Key、CreateRevision、ModRevision、Version、Value、Lease 六个 protobuf 字段均已覆盖。
  CREATE/PUT 使用 inline MVCC metadata 保留 generation 和逐版本 lease，DELETE 的公开 Kv
  仅保留 Key 与删除 ModRevision，完整被删 generation 则进入 PrevKv，均与 etcd watch
  语义一致。新增六字段结构门禁，未来 API 增字段时要求同时更新普通转换与 tombstone 路径。
- 门禁与既有 inline create-revision、PUT previous generation、DELETE tombstone 全字段测试一并
  普通 100 轮、race 20 轮通过；`pkg/server/etcd` 全包、root 全量测试、vet、staticcheck
  v0.7.0 均通过。本轮未发现当前运行时字段遗漏，只增加协议升级失败快显测试，因此不重建
  与 A422 字节相同的生产镜像。

#### A425 KubeBrain 生产 Pod 跨可用区放置偏好

- 生产 TLS/非 TLS StatefulSet 原有三副本、主机级 required pod anti-affinity 和
  `minAvailable=2` PDB，可避免同节点故障，但调度器仍可把三个不同节点全部选在同一 zone，
  无法承受整区故障。两套清单现增加 weight=100、`topology.kubernetes.io/zone` 的 preferred
  pod anti-affinity，并使用完整 instance selector；多 zone 集群优先把副本跨区分散，单 zone
  或节点缺 zone label 时仍可调度，不把软拓扑条件升级为容量不足时的发布阻塞。
- manifest 测试同时固定 hostname required 条目、zone preferred 条目的 weight/topologyKey
  及 name+instance selector，TLS/非 TLS 聚焦普通 100 轮、race 20 轮通过。临时建立空
  `kubebrain-system` namespace 后，两套完整清单均通过 Kubernetes API server dry-run，随后
  namespace 已删除；production 包、root 全量测试、vet、staticcheck v0.7.0 均通过。
- 当前 kind 只有单节点，不能承载既有三 Pod 主机级 required anti-affinity，因此不把单节点
  环境伪装成多 zone 调度证据。该变更只更新生产部署 artifact，不改变 A422 服务端镜像；
  下一次多 zone 预生产发布需以三个 Pod 的 `topology.kubernetes.io/zone` 实际分布和单 zone
  故障注入作为运行门禁。

#### A426 PD/TiKV 生产 Pod 跨可用区放置偏好

- KubeBrain 计算副本跨区并不能单独提供整区容灾：生产 TidbCluster 的三 PD、三 TiKV 原先
  只有主机级 required pod anti-affinity，仍可能集中到同一 zone。PD 与 TiKV 现分别增加
  weight=100、`topology.kubernetes.io/zone` 的 preferred pod anti-affinity，并用各自完整的
  name+instance+component selector；保留 hostname 硬约束和 `minAvailable=2` PDB，同时避免
  zone 容量不足或节点缺 zone label 时让存储集群永久不可调度。
- manifest 测试固定两类存储组件的 preferred 条目数量、weight、topologyKey 及三项 selector；
  聚焦普通 100 轮、race 20 轮通过。完整 TiDBCluster 清单通过已安装 TiDB Operator CRD 的
  Kubernetes API server dry-run；production 包、root 全量测试、vet、staticcheck v0.7.0
  均通过，dry-run 未修改当前集群。
- 当前 kind 为单节点，无法验证跨 zone 实际放置，也不应应用生产 hostname 硬反亲和。该变更
  只更新生产部署 artifact，不改变 A422 服务端镜像；多 zone 预生产发布仍须核验 PD/TiKV
  Pod 的 zone 分布、TiKV region 副本拓扑和整区故障下注写可用性，软调度偏好不能替代这些
  运行门禁。

#### A427 核心 quorum Pod 异常驱逐策略

- 生产 KubeBrain TLS/非 TLS、PD 与 TiKV 的三副本 PDB 均已有 `minAvailable=2`，但此前使用
  Kubernetes 默认 `IfHealthyBudget`：当一个运行中 Pod 已不 Ready、只剩两个健康副本时，
  drain 仍会拒绝驱逐该异常 Pod，可能阻塞故障节点维护和副本在健康节点重建。四类核心 PDB
  现显式设置 `unhealthyPodEvictionPolicy: AlwaysAllow`；只有已不健康 Pod 可越过预算，健康
  Pod 的自愿驱逐仍必须满足两个可用副本。
- TLS/非 TLS KubeBrain 与 PD/TiKV manifest 测试固定该策略和既有 minAvailable；聚焦普通
  100 轮、race 20 轮通过。三套完整清单均通过 Kubernetes API server dry-run，临时
  `kubebrain-system` namespace 已无条件删除并确认无残留；production 包、root 全量测试、
  vet、staticcheck v0.7.0 均通过。
- PDB 不约束节点宕机等非自愿中断，`AlwaysAllow` 也不会恢复已经丢失的 quorum。多 zone
  预生产仍须验证异常 Pod 节点 drain 后的重建、PD/TiKV leader/region 恢复和持续读写；本轮
  只更新生产部署 artifact，不改变 A422 服务端镜像。

#### A428 核心数据面非抢占调度优先级

- 生产 KubeBrain、PD、TiKV 此前未声明 PriorityClass，节点资源压力下会与普通工作负载使用
  默认优先级竞争，PDB 又无法约束 kubelet 的资源压力驱逐。新增平台级共享
  `kubebrain-dbaas-critical` PriorityClass（value=1000000、non-global、
  `preemptionPolicy=Never`），三类核心 Pod 均显式引用；它提高保留和调度排序，但不会通过
  抢占把其他租户工作负载驱逐。
- manifest 门禁固定 class 名称、值、globalDefault、preemptionPolicy、description，以及
  KubeBrain TLS/非 TLS StatefulSet 和 PD/TiKV CR 字段引用。聚焦普通 100 轮、race 20 轮
  通过；临时实际安装 PriorityClass 后，三套完整生产清单均通过 Kubernetes API server
  dry-run，随后 cluster-scoped class 与临时 namespace 均删除且确认无残留；production 包、
  root 全量测试、vet、staticcheck v0.7.0 均通过。
- 生产操作文档要求 class 先于实例资源安装。PriorityClass 不能阻止节点宕机，不能创造容量，
  也不替代 requests/limits、跨 zone 调度、PDB 与 quorum 故障恢复；本轮只更新生产部署
  artifact，不改变 A422 服务端镜像。

#### A429 核心数据面 NetworkPolicy 隔离基线

- 生产核心 namespace 此前没有 NetworkPolicy，任意集群内 Pod 都可尝试访问 KubeBrain client/
  peer、PD API/raft 和 TiKV client/status 端口。新增四条方向独立策略，只选择当前 instance 的
  KubeBrain 或 PD/TiKV：允许同 namespace peer、kube-dns TCP/UDP 53、KubeBrain 到 PD 2379/
  TiKV 20160、默认 `tidb-admin` Operator 到 PD API 2379/TiKV status 20180，以及带显式
  `client-access`/`monitoring-access` namespace 标签的入口；其他 ingress/egress 默认拒绝。
- manifest 门禁固定四个对象、namespace、单一 policyType、目标 instance/component selector、
  namespace selector 和精确端口集合。聚焦普通 100 轮、race 20 轮通过；完整策略通过
  Kubernetes API server dry-run，临时 namespace 删除且确认无残留；production 包、root 全量
  测试、vet、staticcheck v0.7.0 均通过。
- 生产操作文档要求先建/标记 namespace、再应用策略、最后启动工作负载，并要求在真正支持
  NetworkPolicy 的 CNI 上同时执行允许与拒绝探针。当前 kind 环境不能提供 CNI enforcement
  证据，因此本轮不把 schema dry-run 当作网络隔离运行证明；平台若改变 namespace/instance/
  Operator 安装位置必须同步实例化 selector。本轮只更新生产部署 artifact，不改变 A422 镜像。

#### A430 NetworkPolicy CNI enforcement 发布门禁

- 将 A429 的人工允许/拒绝检查实现为 `hack/production/validate-network-policy.sh`。门禁要求
  不可变 digest 探针镜像、唯一 DNS-label probe ID、三个互异 namespace，并在创建 Pod 前
  fail-closed 校验 client/monitoring/denied 标签。它验证 client 到 KubeBrain 3379、monitoring
  到 KubeBrain/PD/TiKV 指标端口、KubeBrain 到 PD/TiKV，以及未标记 namespace 到三类数据面
  的拒绝路径；任一允许失败或拒绝成功都阻止发布。
- 脚本拒绝复用已有同名 Pod，创建成功后立即登记 trap，连 Ready 等待失败也清理部分资源。
  fake kubectl 测试覆盖成功、策略绕过、误阻断、标签缺失、Ready 失败、名称碰撞、mutable
  image 预调用拒绝和 context 传递；普通 100 轮、race 20 轮通过。固定 digest ShellCheck
  v0.11.0、root 全量测试、vet、staticcheck v0.7.0 均通过，并确认生产镜像含 bash/timeout。
- 生产文档给出调用契约并明确脚本只证明 TCP/CNI enforcement，不能替代 endpoint health、
  Put/Get/Delete 或三副本故障验证。当前 kind CNI 未用于伪造通过证据，真实门禁仍须在支持
  NetworkPolicy 的预生产/生产 CNI 上执行；本轮不改变 A422 服务端镜像。

#### A431 核心 etcd 请求 protobuf 字段升级门禁

- 对照 `/root/etcd` d947b2086 与当前 etcd API v3.7 descriptor，逐项审计 Range、Put、
  DeleteRange、Compare、RequestOp、Txn、Compaction、WatchCreate/WatchRequest 及五类 Lease
  请求。现有实现已处理、校验或转发全部当前字段；同时确认 v3.7 `RangeResponse.Count` 不受
  revision filter 影响的合同在普通 Range、CountOnly 和 staged Txn Range 中均保留过滤前总数，
  没有把过滤后 Kvs 长度误作 Count。
- 新增 14 类请求的精确 protobuf descriptor 门禁，不只检查字段数量，还固定每个字段名和 wire
  number，并覆盖 Compare target union 与 RequestOp request union 的全部成员。未来 etcd API
  新增、删除、改名或重编号字段时，测试会要求重新审计 validation、authorization、execution、
  nested Txn 和 follower forwarding，避免生成代码升级后静默忽略新语义。
- 聚焦普通 100 轮、race 20 轮、`pkg/server/etcd` 全包、root 全量测试、vet、staticcheck
  v0.7.0 均通过。本轮确认当前运行时无字段遗漏，只增加 API 升级失败快显门禁，因此不重建
  与 A422 字节相同的服务端镜像。

#### A432 核心 etcd 响应 protobuf 字段升级门禁

- 继续对照 etcd API v3.7 descriptor 审计 ResponseHeader、Range/Put/DeleteRange、ResponseOp、
  Txn、Compaction、Watch 及六类 Lease 响应/状态的所有手工构造、合并、fragment、nested Txn
  和 follower forwarding 路径。当前字段均已覆盖；LeaseGrant 成功响应的 `error` 按上游保持
  空字符串，失败由 gRPC status 返回，不是遗漏。Lease TTL 的 GrantedTTL/Keys 与 LeaseLeases
  的 LeaseStatus ID 也均由同一授权快照填充。
- 新增 14 类响应的字段名+wire number 精确门禁，包含 ResponseHeader 四字段、ResponseOp
  oneof 全成员，以及 WatchResponse events 的非连续 field 11。未来 API 增删、改名或重编号时，
  必须重新审计每个构造器和转换路径，避免新响应字段默认为零值却不触发编译错误。
- 聚焦普通 100 轮、race 20 轮、`pkg/server/etcd` 全包、root 全量测试、vet、staticcheck
  v0.7.0 均通过。本轮未发现当前响应字段遗漏，只增加升级失败快显门禁，因此继续使用已验证
  的 A422 服务端镜像。

#### A433 follower 转发响应 revision 单调性

- 全量差分发现 `StartRevision=math.MaxInt64` 的 Watch 在创建成功、同连接代理 Put、客户端取消后，
  KubeBrain 的 cancel header 仍可能停留在 Put 前 revision；上游 etcd 则至少返回刚完成 Put 的
  revision。根因是 follower 把代理响应直接交给客户端，却没有把响应 header 中已提交的 revision
  回灌本地 committed watermark，而 cancel 为保持 leader 故障时仍可本地完成，不能额外引入读屏障。
- 新增统一的 forwarded response revision 观察逻辑，覆盖 KV Range/Txn/Compact/Put/DeleteRange 及
  Lease Grant/Revoke/KeepAlive/TimeToLive/Leases；只在成功且 revision 为正时单调推进 current
  revision，不推进 published watch watermark。扩展 follower cancel 回归：代理 Put 返回 revision
  61 后，cancel 不新增读屏障且 header 为 61，后续需要 leader 的新 Watch 仍按原合同失败。
- 聚焦 race、`pkg/server/etcd` 全包、root 全量测试、vet、staticcheck v0.7.0 均通过。真实独立
  etcd 对照的 Watch revision boundary 四场景（latest、historical、future、maximum）和隔离重跑的
  naming 差分全部通过；固定 NodePort health 正常且 alarm 为空。`endpoint health --cluster` 从宿主
  runner 仍受集群内广告 DNS 不可解析限制，该拓扑问题不作为协议失败。
- 镜像 `kubebrain:a433-forwarded-revision-local` 从实现提交
  `fa83c80891782e283cde36a079958f52d7dc4afe` 的 `git archive` 构建，本地 image ID 为
  `sha256:18c859f8ba0748870f6f67cdb0edd0fd26d19fd47a2a3918c0064272caecf32a`，kind/containerd
  运行 digest 为 `sha256:afc5edbb59476a9d94d5ff8a3d051bf21dd5f43aeca1fff14695435d315a5504`；
  运行版本 `0.0.0-a433.1`、Go 1.26.5、TiKV backend、用户 `65532:65532`。三 KubeBrain
  副本 Ready、零重启，主 PD/TiKV 3+3 Ready。

#### A434 advertised client URL 真实可达性发布门禁

- 在 A433 镜像上清理旧 restart-persistence 测试遗留的 `[fe,ff)` 高位键后，以全新 reference
  etcd 串行重跑全部普通 Differential 测试。342.004 秒内 binary KV、Alarm、Compact、Delete、
  Hash/HashKV、Lease/leasing、mirror、Namespace/Naming、Put、Range/RangeStream、recipes、STM、
  Txn 和 Watch 均通过；唯一失败仍是 authenticated AutoSync 将宿主 runner 的 bootstrap
  NodePort 替换为集群内 Service DNS 后无法解析。该失败不是协议差异，但证明仅校验 bootstrap
  `ENDPOINT` 和 advertised URL 配置字符串不足以关闭生产发布门禁。
- 提交 `316c6d6` 扩展 `validate-instance-ready.sh`：在原有 immutable image/keyspace/PD/quota/
  advertised args、拓扑收敛和 bootstrap health 检查后，按逗号解析
  `EXPECTED_ADVERTISE_CLIENT_URLS`，从发布门禁所在的真实客户端网络域逐一执行官方
  `etcdctl endpoint health`。空成员或任一替换地址不可达均 fail closed，避免 clientv3
  Sync/AutoSync 在首次 MemberList 后整体断连；多 URL 必须全部通过，不能只探测第一项。
- 专项测试覆盖单/多 URL 成功、第二个 URL 不可达、空成员及既有配置/拓扑/health 失败，普通
  20 轮、race 10 轮通过；`bash -n`、root 全量测试、vet、staticcheck v0.7.0 和 diff check
  均通过。当前集群内客户端网络域直接访问
  `http://kubebrain.kubebrain-dev.svc:3379` 成功提交线性化 proposal；宿主 NodePort health
  同样正常且 alarm 为空，三 KubeBrain 副本 Ready、零重启，主 PD/TiKV 3+3 Ready。
  本轮只加强发布门禁与运行手册，继续使用 A433 数据面镜像。

#### A435 直连三副本拓扑证据与连续 leader replacement

- 通过三个 Pod 独立 port-forward 直连 A433 的三 KubeBrain 副本，Watch 本地控制测试在每个
  member 上均通过 negative revision、empty range、显式 watch ID、duplicate ID 和 client
  cancel；Lease 测试证明每个直连 member 均能读取 live lease/attached key/list，并在统一
  revoke 后观察 key 与 lease 消失。随后连续执行三类真实故障：删除当前 leader 时 follower
  连续完成 current/historical serializable Range 与 serializable Txn；再次删除新 leader 后
  detached/rebound/revoked lease attachment 恢复正确；第三次 leader replacement 时 TTL=1 的
  near-expiry lease 先恢复为存活状态，最终按期限删除 key。全部通过。
- 审计发现 `KUBEBRAIN_DIRECT_ENDPOINTS` 原先只要求三个字符串，同一负载均衡地址重复三次也可
  伪造“跨副本”通过。提交 `039b628` 新增共享 Status 拓扑前置校验：至少三个响应必须具有非零
  cluster/member/leader identity、相同 cluster ID、相同 leader ID、互不重复的 member ID，且
  leader 必须属于被测集合。Watch 与 Lease 直连测试均在业务断言前执行该校验。
- 纯函数测试覆盖有效拓扑、数量不足、nil/zero identity、cluster/leader 分歧、重复 member 和
  集合外 leader，race 20 轮通过。加入真实 NodePort 的兼容模块全量 103.011 秒、模块 vet 和
  staticcheck v0.7.0 均通过。故障后 kubebrain-1/2 均以新 Pod 恢复，三 KubeBrain 副本 Ready、
  零重启，PD/TiKV 3+3 Ready，endpoint health 正常且 alarm 为空；三个临时 port-forward 已
  停止。本轮只加强 HA 证据，不改变 A433 数据面镜像。

#### A436 运行时 MemberList 发布一致性门禁

- A434 只证明 StatefulSet 的 `--advertise-client-urls` 参数与期望一致且期望地址可达，仍可能在
  MemberList 构造回归、静态成员缺失或重复时发布：bootstrap 与公共 URL health 都可成功，
  clientv3 AutoSync 却会收到错误成员集合。真实 A433 MemberList 当前返回同一非零 cluster ID、
  三个唯一 member ID/name、各自稳定 peer URL，并为每个成员返回声明的 client Service URL。
- 提交 `e1f8c6b` 在 `validate-instance-ready.sh` 中使用结构化 `jq` 校验运行时 MemberList：
  cluster ID 非零，成员数精确匹配期望副本数，无 learner，member ID/name 非零非空且唯一，每个
  member 至少一个 peer URL，并且每个 member 的 client URL 集合与
  `EXPECTED_ADVERTISE_CLIENT_URLS` 完全一致。MemberList RPC 失败、JSON 无法解析、成员缺失/
  重复/不完整或 URL 漂移均在逐 URL health 前 fail closed；`JQ` 可由受控工具链覆盖。
- 集成测试新增真实三成员 JSON 生成器，覆盖单/多 advertised URL 成功，以及运行时错误 URL、
  重复 member ID、成员缺失和空 peer URL；连同 A434 矩阵普通 20 轮、race 10 轮通过。
  `bash -n`、root 全量测试、vet、staticcheck v0.7.0 和 diff check 均通过。使用仅存在于 `/tmp`
  的 etcdctl exec 代理，从 `kubebrain-0` 所在真实客户端网络域完整运行发布门禁：TiDB topology、
  KubeBrain rollout/image/immutable args、bootstrap health、三成员 MemberList 和 advertised URL
  health 全部通过。临时代理未进入仓库；本轮不改变 A433 数据面镜像。

#### A437 不可变 KubeBrain peer 拓扑发布门禁

- A436 已锁定运行时成员数和 client URL，但仅要求 peer URL 非空；错误的
  `--initial-cluster` name→peer URL 映射、重复 peer 地址或运行时映射漂移仍可能在三个 Pod
  Ready 且公共 endpoint health 正常时被发布。该拓扑决定选主、follower 转发与成员身份，必须
  与 keyspace、PD 地址一样作为创建后不可变的实例身份，而不能只靠副本数间接推断。
- 提交 `d6374f1` 新增必填 `EXPECTED_INITIAL_CLUSTER`。发布脚本要求 KubeBrain container 恰好
  一个逐字匹配的 `--initial-cluster`，结构化解析逗号分隔 member 与分号分隔 peer URL，拒绝
  空/重复 member name、空/重复 peer URL，并要求成员数精确匹配期望 KubeBrain 副本数；运行时
  MemberList 的排序归一化 `{name, peerURLs}` 集合必须与该 immutable 声明完全一致。成功审计
  记录同时输出 initial cluster，便于控制面追溯扩缩/升级实际绑定的 peer 拓扑。
- 集成测试覆盖参数缺失/错误、重复 peer URL、运行时错误 peer 映射，以及 A436 的成员/client
  URL 全矩阵；专项普通 20 轮、race 10 轮通过。`bash -n`、root 全量测试、vet、staticcheck
  v0.7.0 和 diff check 均通过。使用临时 in-cluster etcdctl exec 代理完整运行真实发布门禁，
  StatefulSet 的三成员 initial cluster 与 MemberList 三个稳定 Pod peer DNS 精确一致，bootstrap、
  advertised endpoint health 和 PD/TiKV 3+3 topology 全部通过；临时代理已删除。本轮不改变
  A433 数据面镜像。

#### A438 不可变 TiKV/PD cluster identity 发布门禁

- A382 已锁定 `--pd-addrs` 字符串，但稳定 Service DNS 仍可能被错误重指向另一套 PD/TiKV；
  keyspace、参数、Pod readiness 和 endpoint health 均可能继续通过，实例却已进入错误存储故障
  域。TiKV backend 的 response cluster ID 来自 PD `GetClusterID`，当前 TidbCluster status、PD
  API 和 KubeBrain Status/MemberList 均报告 `7662961163671170154`，可作为实例创建 receipt
  中独立于 DNS 的 immutable storage identity。
- 提交 `0b5f0df` 新增必填正整数 `EXPECTED_CLUSTER_ID`。发布门禁首先要求 TidbCluster
  `.status.clusterID` 精确匹配，再要求 KubeBrain 运行时 MemberList header 的 cluster ID 精确
  匹配；任一缺失或漂移均 fail closed。比较通过 `jq -r ... | tostring` 后做字符串等值，不把
  64 位 ID 转成 shell/JSON 浮点，避免超过 2^53 后精度丢失。成功审计记录同时输出 cluster ID。
- 集成测试覆盖缺失输入、TidbCluster cluster ID 错误和 KubeBrain runtime cluster ID 错误，
  连同现有发布矩阵普通 20 轮、race 10 轮通过。`bash -n`、root 全量测试、vet、staticcheck
  v0.7.0 和 diff check 均通过。真实 in-cluster 完整门禁使用上述 64 位 ID 成功，证明
  TidbCluster 与 KubeBrain 两侧身份一致且无精度截断；三 KubeBrain 副本 Ready、零重启，
  endpoint health 正常且 alarm 为空。临时 exec 代理已删除，本轮不改变 A433 数据面镜像。

#### A439 TidbCluster Kubernetes UID 发布 fencing

- A438 绑定 PD cluster ID 后，发布门禁仍无法区分同名 TidbCluster CR 被删除重建的控制面
  replacement。即使新对象最终连接同一 PD cluster，旧 operation/approval 若继续推进也会越过
  Kubernetes ownership 边界；仓库的 snapshot/restore/destroy 状态机已经使用 UID fencing，
  普通实例发布必须采用同一原则。当前受管 `tidb-cluster/kb` UID 为
  `c690fd02-b7b9-4335-b3e8-acdf2b8e5706`。
- 提交 `ff15d8d` 新增必填 `EXPECTED_TIDB_CLUSTER_UID`。发布门禁在 topology 收敛后读取
  TidbCluster metadata UID，必须与实例状态或 operation receipt 中的期望值逐字匹配，再继续
  PD cluster ID、KubeBrain rollout/MemberList 和 endpoint 检查；同名 CR replacement 因而在
  数据面发布前 fail closed。成功审计记录同时输出 UID 与 cluster ID，分别证明 Kubernetes
  resource ownership 和底层 PD storage identity。
- 集成测试覆盖缺失 UID 与实际 UID 漂移，连同完整 release gate 矩阵普通 20 轮、race 10 轮
  通过；`bash -n`、root 全量测试、vet、staticcheck v0.7.0 和 diff check 均通过。真实
  in-cluster 门禁使用上述 UID 和 64 位 cluster ID 完整通过，三 KubeBrain 副本 Ready、零重启，
  endpoint health 正常且 alarm 为空。临时 exec 代理已删除，本轮继续使用 A433 数据面镜像。

#### A440 KubeBrain StatefulSet Kubernetes UID 发布 fencing

- A439 已锁定存储侧 TidbCluster UID，但同名 KubeBrain StatefulSet 删除重建后，旧的 create/
  scale/upgrade operation 仍可能误把新对象判定为自己的 rollout 并继续发布。当前受管
  `kubebrain-dev/kubebrain` UID 为 `817bc005-a4d1-4d57-9aab-de93e0874054`，必须与操作 receipt
  中记录的资源身份绑定，形成数据面与存储面两侧对称的 Kubernetes ownership fencing。
- 提交 `8b411cd` 新增必填 `EXPECTED_KUBEBRAIN_STATEFULSET_UID`。发布门禁在读取 StatefulSet
  rollout 状态时同时读取 metadata UID，要求与 receipt 逐字匹配；同名对象 replacement 即使
  generation、revision、镜像、参数和副本数全部收敛也会 fail closed。成功审计同时输出
  KubeBrain StatefulSet UID、TidbCluster UID 和 PD cluster ID。
- 集成测试覆盖缺失 UID 与实际 UID 漂移，连同完整 release gate 矩阵普通 20 轮、race 10 轮
  通过；`bash -n`、root 全量测试、vet、staticcheck v0.7.0 和 diff check 均通过。真实
  in-cluster 门禁使用上述 StatefulSet UID、TidbCluster UID 和 64 位 cluster ID 完整通过，
  bootstrap 与 advertised endpoint 均成功提交线性化 proposal；三 KubeBrain 副本 Ready、
  零重启，PD/TiKV 3+3 Ready。临时 exec 代理已删除，本轮继续使用 A433 数据面镜像。

#### A441 逐 Pod rollout 与 ownership 发布证据

- A440 将 release operation 与 StatefulSet UID 绑定，但 StatefulSet 的聚合 ready/updated 状态
  不能单独证明实际 serving Pod 都来自该对象的当前 ControllerRevision：旧 owner Pod、旧 revision
  Pod、terminating Pod 或非 Ready Pod 在标签/状态异常窗口内仍可能让旧 operation 越过发布阶段。
- 在 `8b411cd` 之后，发布门禁进一步结构化读取稳定应用标签选择的 Pod 集合，要求名称精确覆盖
  `${StatefulSet}-0..N-1`；每个 Pod 必须为 Running/Ready、无 deletion timestamp、带当前
  StatefulSet update revision、唯一的目标 KubeBrain 镜像，并有唯一的 apps/v1 StatefulSet
  controller owner 指向 immutable `EXPECTED_KUBEBRAIN_STATEFULSET_UID`。任一 Pod 与聚合状态
  分歧均 fail closed。
- 集成测试覆盖旧 StatefulSet owner、旧 controller revision、terminating 与非 Ready Pod；专项普通
  20 轮、race 10 轮，root 全量测试、vet、staticcheck v0.7.0 和 diff check 均通过。真实
  in-cluster 门禁验证三 Pod 的 owner UID、`kubebrain-bc6b9b5df` revision、镜像与 Ready 状态，
  bootstrap/advertised endpoint health 均成功提交线性化 proposal；临时 exec 代理已删除，继续使用
  A433 数据面镜像。

#### A442 client Service 与 EndpointSlice 发布 fencing

- A441 逐 Pod 证明 rollout 已收敛，但 shared advertised client Service 的 selector 或 EndpointSlice
  仍可能漂移到旧/缺失 Pod；此时 MemberList 与单次 Service health 可能成功，流量却无法覆盖完整
  数据面副本。
- 新增必填 `EXPECTED_KUBEBRAIN_CLIENT_SERVICE_UID`，发布门禁将 client Service resource identity
  与 operation receipt 绑定，并要求该 Service 的全部 EndpointSlice endpoint 都是 ready/serving、
  非 terminating 的 Pod targetRef，且 targetRef UID 集合与 A441 验证的当前 Pod UID 集合精确相同。
  Service replacement、旧 endpoint 或缺失副本均 fail closed。
- 集成测试覆盖旧 endpoint Pod UID；专项普通 10 轮、race 3 轮、root 全量测试、vet、staticcheck
  v0.7.0 和 diff check 均通过。真实 in-cluster 门禁使用 client Service UID
  `42cae35c-f9e4-4eb9-91d7-9e17fdaea3fe` 完整通过，EndpointSlice 精确引用三枚当前 Pod；临时
  exec 代理已删除，继续使用 A433 数据面镜像。

#### A443 TiKV uncertain commit 与 quorum recovery

- 真实 3 PD/3 TiKV quorum smoke 曾在单 TiKV replacement 时暴露 `TxnLockNotFound` 被透传为
  gRPC `Unknown`。client-go v2.0.7 不提供该 2PC commit 错误的 typed wrapper；commit outcome
  可能已经落盘，不能作为确定性失败处理。
- 提交 `5263a1c` 将含精确 `TxnLockNotFound` 标记的 TiKV commit error 归类为
  `storage.ErrUncertainResult`，交给已有 event-log durable resolver 判定 committed/not-committed；
  公共 unary interceptor 将 uncertain result 映射为 etcd `ErrGRPCTimeout`（gRPC `Unavailable`），
  使 clientv3 以暂态、结果不确定写处理，而不会把可能已提交事务误报为 `Unknown`。quorum smoke
  还将 Pod Ready 后的 PD region cache/KubeBrain readiness 收敛观察窗口参数化，默认 10 秒。
- A444 修复 PrevHint/revision cache 的 eager map 预分配：代际 map 的初始容量限制为 8192，
  逻辑上的 entry cap 与 byte cap 不变，map 仍按实际工作集扩张。此前字节边界测试用百万 entry
  cap 来证明 byte rotation，会在构造和每次 rotation 分配百万 buckets，race instrumentation 下
  使完整套件超时；修复后同一测试只为实际保留的少量大对象分配 buckets。
- 同时修复旧代际命中提升绕过双边界的问题：提升前使用与写入一致的 rotation 判定，避免高命中
  工作集在无新写入时持续把 live generation 推过 entry/byte 上限。两类 cache 的容量为 1 回归
  固定提升后 live generation 仍只有一个条目，且被轮转出的条目仍可读取。
- 定向 byte-bound race 20 轮，以及完整 `go test -race ./pkg/server/etcd -count=1` 均通过，后者
  在两次独立运行中耗时 223.685 秒和 224.046 秒；普通 root 全量测试、vet、staticcheck v0.7.0
  和 diff check 也均通过。
  从 `5263a1c` 干净 archive 构建的 `kubebrain:a444-uncertain-commit-local` 滚动三副本后，真实
  PD leader replacement 在故障窗口完成 1006 次操作，TiKV member replacement 完成 1001 次，
  两者均通过并最终 TidbCluster Ready=True。
- `c893725` 的静态 TiKV 二进制以 `kubebrain:a445-cache-bounds-local` 滚动三副本后，集群内
  UID-fenced release gate 验证 StatefulSet/Service/EndpointSlice 精确身份、3 PD/3 TiKV、
  MemberList、advertised client URL 与线性化 proposal；随后真实 PD leader replacement 完成 952
  次操作、TiKV member replacement 完成 1096 次操作，均通过且 TidbCluster 最终 Ready=True。
- A445 的初次规范镜像 quorum smoke 暴露测试端假阴性：clientv3 将已知 etcd timeout 转成
  `rpctypes.EtcdError`，其只有 `Code()` 而不实现 gRPC `GRPCStatus()`，原 smoke 仅用
  `status.Code`，错误把 `etcdserver: request timed out` 判为致命。`isMutationFailoverAmbiguous`
  现先通过 `errors.As` 读取该官方 `Code()` 契约，再检查 gRPC status；普通 50 轮、race 20 轮
  固定 `ErrTimeout`、wrapped leader-fail timeout 与 non-transient lease-not-found 的边界。
- 完整 Dockerfile 已从同一干净 archive 成功构建规范镜像
  `kubebrain:a445-canonical-local`（image ID
  `sha256:581bb43e44027cf0ae078cbf23b1f2a2363ae26eb06002b4ab62b77e0c1d7ae7`，Git SHA
  `c8937254185a7fb6b608da26eada5765a01bc071`，Go 1.26.5）。该镜像滚动三副本并通过集群内
  UID-fenced release gate；修复 smoke 后 PD leader replacement 完成 1133 次操作、TiKV member
  replacement 完成 1140 次操作，均通过且最终 Ready=True。指定真实 NodePort 的完整 compat
  module 为 119.633 秒通过，兼容模块 vet/staticcheck v0.7.0 同时通过。
- A446 将 release gate 的 cluster-local advertised URL 验证纳入正式契约：可选
  `ETCDCTL_EXEC_POD`（以及 namespace/container）通过指定 Pod 执行 endpoint health 与
  MemberList，直接宿主 `ETCDCTL` 路径仍为默认。参数以 argv 传递，Pod 执行不注入已废弃的
  `ETCDCTL_API` 环境变量。fake kubectl 门禁固定 exec pod、`--` 分隔和 etcdctl 参数透传；普通
  20 轮、race 10 轮通过。真实 A445 三副本 gate 使用 `ETCDCTL_EXEC_POD=kubebrain-0` 完整通过，
  从同一 client 网络域验证 advertised service URL、MemberList 和 endpoint health，无需临时
  未版本控制的代理脚本。
- A447 将既有 repeated-leader lease renewal soak 固化为
  `hack/dev/lease-renewal-failover-smoke.sh` 并补到规范 A445 运行证据：脚本通过 endpoint status
  的 leader member ID 与 MemberList 名称在每轮动态选择 leader，只允许删除目标 StatefulSet 的
  Pod，且先要求 3/3 Ready。8 个 client 的 64 条
  `clientv3.KeepAlive` lease 跨连续 3 次 leader Pod replacement 持续获得有效 TTL 响应；每轮
  都核验所有 attached key/lease ID，结束后逐 lease Revoke、TTL=-1、Leases 列表和前缀均无残留。
  脚本在真实 A445 集群以可审计退出码 54.031 秒通过，三副本最终 Ready、零容器重启。该加速场景不替代小时级、跨可用区或
  网络分区 soak，但把 A445 的 lease reload/renewal 故障边界固定为可重复现场门禁。
- A448 扩展同一 leader-delete helper 的 endpoint 契约：未显式设定 `ENDPOINT` 时继承
  `KUBEBRAIN_ETCD_ENDPOINT`，使 compat failover command 不会危险地回退到本地默认地址。真实 A445
  上的 1200 条长 TTL lease promotion-spread 测试与 empty lease Revoke-after-failover 测试均通过，
  总耗时 49.076 秒；前者证明恢复后的到期时间被分散，后者证明已删除的 metadata 不会在 leader reload
  后复活。
- A449 在真实 A445 集群完成 long-lease checkpoint failover：600 秒 lease 等待至剩余 TTL 不超过
  285 秒后删除当前 leader，replacement leader 恢复的 TTL 仍不超过 310 秒、GrantedTTL 仍为 600，且
  attached key 完整可读；测试以可审计退出码在 321.067 秒通过。该结果覆盖五分钟 checkpoint 周期后的
  恢复语义，排除了 promotion 将 lease 错误重置为完整 grant TTL 的风险。
- A450 在真实 A445 集群动态识别当前 leader 后，于 3 秒 lease 仅剩 1 秒时删除该 Pod；near-expiry
  promotion 测试以可审计退出码在 4.981 秒通过。故障窗口有两次客户端 `DeadlineExceeded`，随后 lease
  与 attached key 均恢复可读并最终自然过期删除，证明 promotion 不会因短暂不可用提前遗失临期 lease。
- A451 在 A449/A450 的 leader replacement 后重新执行完整 in-cluster release gate 并通过：TidbCluster
  为 Ready，3/3 KubeBrain replicas、实例与 Service UID、canonical image、keyspace、PD 地址、cluster ID、
  initial cluster、quota 和 advertised client URL 全部匹配，cluster-local endpoint health 同时通过。
- A452 在真实 A445 集群重跑 in-cluster clientv3 balancer gate：固定连接 `kubebrain-0` 后替换该
  Pod，官方 client 自动切换三 endpoint，连续收到 20 个严格递增 Watch event（末 revision
  `467848433245618202`），线性/可串行读、Txn、Put 与 Delete 均通过且 transient failures 为 0；
  replacement Ready 且临时 Job/RBAC 由脚本清理。
- A455 用 `/root/etcd` 临时 reference 实例重跑 destructive differential，发现 `math.MaxInt64`
  Watch 在写入后由客户端取消时，KubeBrain 的 cancel header 仍取 event-published watermark，而
  reference etcd 已返回 committed revision。现 client cancel 保持无 read barrier，但取 current revision；
  确定性 published=70/current=71 回归、完整 etcd server 普通测试、race（235.673 秒）、vet 与
  Staticcheck 均通过。干净 `dc23b47` archive 构建的 A455 镜像在真实 TiKV/PD 集群滚动到 3/3 后，
  四类 Watch revision boundary 的 reference differential 在 1.447 秒完全通过，完整 release gate
  亦通过。差分 runner 同时支持从指定 Pod 检查 cluster-local MemberList URL，并将宿主 reference
  `ETCDCTL_BIN` 与 Pod 内 `ETCDCTL_EXEC_BIN` 分离；host NodePort client 的 AutoSync 仍必须配置对
  该网络域可达的 `--advertise-client-urls`，不能把开发集群内 Service URL 当作外部发布地址。
- A456 将专用 destructive differential 实例的 advertised client URL 调整为宿主机和 Pod 均可达的
  `http://172.18.0.2:30079`，保持 A455 镜像与 StatefulSet/Service/TidbCluster UID 不变。完整
  `run-differential.sh` 在 309.082 秒以退出码 0 通过；此前受网络域不匹配影响的 authenticated
  AutoSync 已通过，Watch revision boundary 的 latest-zero、historical-current、future-next 和
  maximum 在 reference etcd 与 KubeBrain 两侧也全部通过。随后完整 release gate 再次确认 3/3
  KubeBrain、3/3 PD/TiKV、镜像、keyspace、PD 地址、cluster ID、initial cluster、quota 与 advertised
  URL 均匹配，宿主机及 Pod 内使用同一 NodePort 的 endpoint health 均成功。该结果把 A455 的定向
  Watch 修复证据提升为当前生产拓扑下的全量差分与部署门禁证据；被显式跳过且要求独立实例的 quota、
  compaction、auth mirror 等场景仍由各自专用门禁覆盖。

### P1：通用服务能力

1. 继续扩大 Auth 差分、token/证书轮换和长连接故障验证；管理 API、key-range
   RBAC、token 生命周期、Watch/Lease 持续鉴权、客户端证书 CN 身份以及真实三副本
   auth+mTLS failover、服务端/内部客户端叶证书热轮换、CA trust pool 双信任窗口与撤旧
   及真实三副本在线轮换、长连接 drain/reconnect soak、CRL/cipher/TLS version 策略、
   独立 outbound client cert/key 与 peer CN/SAN allowlist 已完成；下一步扩大配额与故障
   注入覆盖。client RPC 并发限额和 logical Watch 配额均已覆盖真实副本 UID
   replacement 后的计数释放。
2. `client/v3/concurrency` mutex/election/session、lease 自然过期和 failover
   recipe 已通过；64 lease/8 client/3 次连续 leader replacement 的加速 renewal
   soak 已建立，继续增加小时级和跨可用区 soak。
3. DBaaS 创建、扩缩、升级的数据面 release gate、备份完成 gate、Object Lock
   上传/保留删除、恢复验证 receipt、UID-fenced 流量切换、恢复后持续审计、证书轮换
   gate、UID-fenced 销毁状态机及持久 operation API/worker fencing、Backup/
   RestoreCutover/CertificateRotation/Destroy/BackupDeletion executors、专属
   namespace/凭据外围 UID-fenced 清理和 backup exact-version bucket lifecycle operation
   已建立；exact-version inventory 对账和 HA-safe 定期策略触发已完成；继续补跨账户保留规划；
   Kubernetes 原生提交者与 worker 最小权限身份已分离，单 namespace 跨实例公平调度和
   实例互斥、终态
   operation 的 Object Lock 不可变归档及 finalizer/删除门禁均已完成；继续补外部管理
   API 的 OIDC/tenant/instance 授权入口和显式 allowlist 多 namespace 定期调度已完成；
   继续补跨 bucket/account 汇总、跨 cluster/region 全局调度
   和管理面/外部 IdP HA soak。
4. 建立实例级限额和计量：请求字节、txn 操作数、跨连接 client RPC 总并发、client
   请求 QPS/burst、逻辑 Watch 总数、CPU/内存饱和、网络错误/丢包及 PD/TiKV PVC 容量
   告警已具备稳定错误或指标，网络 RX/TX 原始计量和逻辑备份 artifact 容量/新鲜度
   指标已暴露，单实例资源/容量/网络/备份 recording rules 和 fail-closed 缺测标记已
   建立，不可变小时采样、counter-based 小时窗口、确定性跨日积分、不可变资源价格
   版本、对象 storage byte-time、对象请求费、v3 charge、approved adjustment/credit
   和 final invoice 已完成；供应商账单周期对账的 provider statement /
   provider reconciliation canonical artifact、跨 account/bucket 分类汇总、Object
   Lock 读写 receipt 绑定、normalized CSV 导入/发布和生产镜像 CLI 入口已建立；invoice
   payment/refund/chargeback ledger 与 remaining balance canonical artifact 已建立；外部总账
   ingest 用 balanced journal export artifact 已建立；继续补各云厂商原始账单 exporter、
   税率/折扣、真实支付渠道、税务/开票系统实际回执、ERP/GL 实际过账回执与跨账户财务对账，
   并在具备 Prometheus Operator 的预生产环境补真实
   一小时规则 evaluation 和连续 24 小时 storage sampling 门禁。

- A457 新增 `kubebrain.metering-provider-statement.v1` 和
  `kubebrain.metering-provider-reconciliation.v1`：供应商 statement 必须按 provider、
  account、外部 invoice/line、分类、object store 和 bucket 排序且逐行合计；reconciliation
  绑定 final invoice 与 provider statement 的不可变 Object Lock read receipt，聚合跨
  account/bucket 成本分类并计算 customer invoice total、provider statement total 与
  gross margin。`kubebrain-metering-provider-reconcile` 已进入 Dockerfile build/copy 链路；
  `go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。该项关闭周期级供应商账单对账
  的本地制品合同，真实供应商账单导入/exporter、税率/折扣/收付款、外部总账和跨账户财务
  对账仍属 P1。
- A458 新增 normalized provider statement CSV 导入与发布入口：CSV header 必须精确匹配
  `provider,provider_account_id,external_invoice_id,external_line_id,category,object_store_id,bucket,amount_micros`，
  每行先做分类、bucket scope 与金额校验，再按 provider/account/external line/category/object
  store/bucket 稳定排序并写出 canonical `kubebrain.metering-provider-statement.v1`。发布器在
  Object Lock 写入前重新读取 canonical JSON，校验 issued_at、retention、digest、bytes 与
  receipt；`kubebrain-metering-provider-statement` 已进入 Dockerfile build/copy 链路，并可选择
  只生成 JSON 或生成后直接归档。`go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。该项关闭 KubeBrain 侧 normalized
  供应商账单导入/发布合同；AWS/GCP/Azure 等原始账单 exporter 与税务/财务系统集成仍属 P1。
- A464 新增 `kubebrain.metering-payment-ledger.v1` 和
  `kubebrain-metering-payment-ledger`：外部收款系统导出的 normalized CSV header 固定为
  `processor,external_transaction_id,kind,amount_micros,occurred_at_unix`，kind 只允许
  payment/refund/chargeback，交易按 processor+外部交易 ID 排序且唯一。ledger 绑定 finalized
  invoice 的 exact-version source、invoice total、交易明细、payments/refunds/chargebacks/net
  paid 与 remaining balance；未来交易、重复交易、退款/chargeback 超过已收款、net paid 超过
  invoice total 或 invoice source 漂移均 fail closed。publisher 在 Object Lock 写入前重新读取 canonical
  JSON，核验 invoice source retention 和上传 receipt；Dockerfile build/copy 已包含该 CLI。
  `go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。该项关闭 KubeBrain 侧应收账款状态
  制品合同，不替代真实支付渠道、税引擎、发票编号法规或外部总账过账。
- A465 将 payment ledger 生产路径从“调用方提供 invoice/source JSON”收紧为 exact-version
  invoice read：`kubebrain-metering-payment-ledger` 在未显式提供离线 `--invoice/--invoice-source`
  时，会按 instance/invoice ID 从 Object Lock 读取 finalized invoice，校验 read receipt 与下载
  字节 digest/bytes，再把该 receipt 转成 ledger 内的 invoice source。processor 同时要求 invoice
  source 至少保留到 ledger retention 边界，并在 archive 前复读 canonical ledger；read receipt
  与下载字节不一致时不会写 ledger，也不会发布 Object Lock artifact。回归覆盖 exact-read 后归档
  和 receipt/bytes 漂移 fail-closed；文档也将约束表述修正为 net paid 不得超过 invoice total。
  `go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。该项减少手工 source 拼装风险，仍不替代
  真实支付渠道或外部总账过账。
- A466 收紧 payment ledger 离线演练路径：新增
  `BuildPaymentLedgerFromCSVWithInvoiceStatus`，要求调用方提供的 invoice source 的 SHA-256 与
  bytes 精确等于本地 canonical invoice 文件；`kubebrain-metering-payment-ledger --invoice
  --invoice-source` 也改走该 helper。这样 source JSON 即使格式、artifact ID 和 retention 合法，
  只要不是这份 invoice 字节的 exact-version source 就会 fail closed。focused negative test 覆盖
  source digest 漂移；`go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。该项把离线恢复/财务演练的证据合同
  与生产 exact-read 路径对齐。
- A467 新增 `kubebrain.metering-general-ledger-export.v1` 与
  `kubebrain-metering-ledger-export`：exporter 从 Object Lock exact-read finalized invoice、可选
  provider reconciliation 和可选 payment ledger，校验 read receipt 与下载字节、source retention
  以及所有输入的 invoice source 一致后，生成 canonical balanced journal。invoice 生成
  accounts receivable/revenue，provider allocation 生成 provider cost/accounts payable，
  payment/refund/chargeback 生成 cash/accounts receivable；每条 line 绑定 source format/ID/source
  line，debit total 必须等于 credit total。publisher 在 archive 前复读 canonical export 并校验
  Object Lock receipt；Dockerfile build/copy 已包含该 CLI。回归覆盖借贷平衡、输入漂移拒绝、
  exact-read 三类制品后归档；`go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。该项建立 KubeBrain 侧外部总账 ingest
  证据合同，不替代真实 ERP/GL posting、period close、账号映射审批或反向回执。
- A468 收紧 general ledger export 读回校验：`GeneralLedgerExport.Validate` 现在构造顶层
  invoice/provider reconciliation/payment ledger source allowlist，并要求每条 journal line 的
  source format/ID 必须命中该 allowlist。这样攻击者即使保持 debit/credit 平衡，也不能把某条
  分录改指向未绑定 artifact。新增负例只篡改 line source ID、总额不变，读回必须失败；
  `go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。
- A469 收紧 general ledger export canonical line ordering：`Validate` 现在不仅对 line 副本排序
  后计算 debit/credit，还要求原始 artifact 的 `lines` 数组已经按 line ID 递增排列。这样即使
  分录内容、source allowlist 和借贷总额都合法，重排 line 数组也不能作为 canonical export
  读回通过。新增负例交换两条 line、总额不变但必须失败；`go test ./hack/production/... ./deploy/production`
  与 `go vet ./hack/production/... ./deploy/production` 通过。
- A470 新增 `kubebrain.metering-invoice-number-assignment.v1` 与
  `kubebrain-metering-invoice-number`：assigner 从 Object Lock exact-read finalized invoice，
  校验 read receipt 与下载字节、source retention 和 invoice identity 后，固化 jurisdiction、
  approved series、positive sequence、`series-sequence-12digit.v1` display number、invoice total
  与 invoice source。display number 被篡改、source 漂移、assignment 早于 invoice finalized、
  未来 assignment 或 retention 不足都会 fail closed；Dockerfile build/copy 已包含该 CLI。
  focused tests 覆盖编号生成/读回、source/display 篡改和 exact-read 后归档；
  `go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。该项建立 KubeBrain 侧法规发票编号
  assignment 证据合同，不替代当地法规校验、号段审批、作废/红冲流程或开票平台回执。
- A471 收紧 invoice number assignment 的 invoice source 字节绑定：新增
  `BuildInvoiceNumberAssignmentWithInvoiceStatus`，要求 source SHA-256 与 bytes 精确等于本地
  canonical invoice status；生产 assigner 也改走该 helper，使 Object Lock exact-read 路径和
  直接构建路径共享同一 fail-closed 证据合同。新增负例篡改合法 source 的 digest 后必须拒绝
  生成 assignment，避免内部 API 被错误 exact-version source 复用。`go test
  ./hack/production/internal/meteringbilling -run 'Test.*InvoiceNumber' -count=1`、
  `go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。
- A472 收紧 general ledger export 的输入 source 字节绑定：新增
  `BuildGeneralLedgerExportWithStatuses`，要求 invoice、可选 provider reconciliation、可选
  payment ledger 的 source SHA-256/bytes 均精确等于对应本地 canonical artifact status；生产
  exporter 改走该 helper，继续在 Object Lock read receipt 校验之外保留直接构建路径的
  fail-closed 门禁。新增回归覆盖 invoice/reconciliation/payment 三类合法 source digest 漂移；
  `go test ./hack/production/internal/meteringbilling -run 'Test.*GeneralLedger' -count=1`、
  `go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。
- A473 收紧 provider reconciliation 的 upstream source 字节绑定：新增
  `BuildProviderReconciliationWithStatuses`，要求 provider statement source 与 finalized invoice
  source 的 SHA-256/bytes 分别精确等于本地 canonical statement/invoice status；生产
  `kubebrain-metering-provider-reconcile` 路径改走该 helper，在 Object Lock read receipt
  校验之外也保护直接构建入口。新增回归覆盖 provider statement source digest 漂移和 invoice
  source digest 漂移；`go test ./hack/production/internal/meteringbilling -run
  'Test.*ProviderReconciliation|Test.*ProviderReconciler' -count=1`、`go test
  ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。
- A474 收紧 finalized invoice 的 upstream source 字节绑定：新增 `BuildInvoiceWithStatuses`，
  要求 invoice plan、每个 charge、每个 adjustment 的 source SHA-256/bytes 分别精确等于
  对应本地 canonical artifact status；生产 `kubebrain-metering-invoice-finalize` 路径改走该
  helper，继续保留 plan 顺序、adjustment charge-source、signed adjustment total 与非负总额
  校验。新增回归覆盖 plan/charge/adjustment 三类合法 source digest 漂移；
  `go test ./hack/production/internal/meteringbilling -run 'Test.*Invoice' -count=1`、
  `go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。
- A475 收紧 metering charge 的 upstream source 字节绑定：resource rollup 新增
  `ReadRollupStatus`，charge builder 新增 `BuildChargeWithStatuses`、`BuildChargeV2WithStatuses`
  和 `BuildChargeV3WithStatuses`，要求 resource rollup、可选 storage rollup 与 catalog source
  的 SHA-256/bytes 分别精确等于本地 canonical artifact status；生产
  `kubebrain-metering-charge` 路径改走这些 helper。新增 v3 回归覆盖 resource/storage/catalog
  三类合法 source digest 漂移；`go test ./hack/production/internal/meteringbilling -run
  'Test.*Charge' -count=1`、`go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。
- A476 收紧 settlement publisher 的本地输入冻结：`kubebrain-metering-settlement-publish`
  在 strict/canonical、审批时间和 retention 校验后，将 adjustment/plan 重写到临时 canonical
  frozen copy，并只把该 frozen path 交给 Object Lock executor；即使原始 `--input` 在 executor
  调用期间被替换，也不会上传错误字节。新增回归在 executor 回调中篡改原始 plan 文件，确认
  上传路径不是原始输入且 receipt 仍绑定已校验 canonical bytes；`go test
  ./hack/production/internal/meteringbilling -run 'TestSettlementPublisher' -count=1`、
  `go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。
- A477 收紧本地输入型 financial publisher 的上传冻结：price catalog、provider statement 和
  payment ledger publisher 均在 canonical/retention/source 校验后重写临时 canonical frozen copy，
  并只把 frozen path 交给 Object Lock executor，避免原始 `--input` 在 executor 调用期间被替换后
  污染不可变对象。现有 publisher 回归扩展为在 executor 回调中篡改原始输入，再读取 frozen
  input 生成 receipt，覆盖三类 artifact；`go test ./hack/production/internal/meteringbilling -run
  'TestPublisher|TestProviderStatementPublisher|TestPaymentLedgerPublisher' -count=1`、
  `go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。
- A478 收紧生成型 financial archive 的输出冻结：`kubebrain-metering-invoice-number` 和
  `kubebrain-metering-ledger-export` 在写入用户 output 后，会再把同一 assignment/export 重写到
  临时 canonical frozen archive copy，并只把该 frozen path 交给 Object Lock executor。这样用户
  output path 即使在 executor 调用期间被替换，也不会上传错误字节；receipt 仍绑定写出时的
  canonical status。回归在 executor 回调中篡改 output path，再从 frozen input 生成 receipt，覆盖
  invoice number assignment 与 general ledger export；`go test
  ./hack/production/internal/meteringbilling -run 'Test.*InvoiceNumber|Test.*GeneralLedger' -count=1`、
  `go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。
- A479 收紧 payment ledger processor 到 publisher 之间的 output path 冻结：processor 写出用户
  output 后，会再写内部 canonical `payment-ledger.publish.json`，并把该 frozen input 传给
  publisher；publisher 继续生成自己的 frozen upload copy。这样用户 output path 即使在 archive
  executor 调用期间被替换，也不会影响 Object Lock 上传。回归在 processor archive 回调中篡改
  output path，再从 archived bytes 读回 ledger；`go test
  ./hack/production/internal/meteringbilling -run 'TestPaymentLedgerProcessor' -count=1`、
  `go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。
- A480 收紧 object storage metering sample/rollup archive receipt 校验：sample archive
  receipt 必须精确等于请求 retention mode，rollup archive receipt 新增 trailing JSON
  拒绝并精确绑定请求 retention mode。这样 Object Lock executor 返回合法但非本次请求策略的
  COMPLIANCE/GOVERNANCE receipt，或在 rollup receipt 后拼接额外 JSON，都会 fail closed。
  回归覆盖 sample retention mode drift、rollup retention mode drift 和 rollup trailing JSON；
  `go test ./hack/production/internal/meteringstorage -run 'Test.*ArchiveReceipt' -count=1`、
  `go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。
- A481 收紧 object storage metering rollup 的 sample read receipt 保留窗口：Roller 现在把
  每小时 `MIN_RETAIN_UNTIL_UNIX` 作为本地 parser 输入，并拒绝 retain-until 早于
  `slot_end + retention_duration` 的 sample read receipt。这样 executor 返回的 sample 即使
  digest/bytes 匹配且覆盖当天 period，只要保留窗口低于配置证据保留期，也不会进入
  rollup source evidence。回归覆盖 retain-until 低于下限的 read receipt；
  `go test ./hack/production/internal/meteringstorage -run 'Test.*ReadReceipt|Test.*Rollup' -count=1`、
  `go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。
- A482 收紧 resource metering rollup 的 sample read receipt 保留窗口：旧资源计量 Roller
  同样把每个小时的 `MIN_RETAIN_UNTIL_UNIX` 交给本地 parser，并拒绝 retain-until 低于
  `slot_end + retention_duration` 的 sample read receipt。这样 resource rollup 不再只依赖
  executor 或后续 period-end 覆盖校验证明源保留期。回归覆盖 retain-until 低于下限的
  read receipt；`go test ./hack/production/internal/meteringarchive -run 'Test.*ReadReceipt|TestRoller' -count=1`、
  `go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。
- A483 收紧 billing Object Lock 写入 receipt 的 retention mode 绑定：共享
  `parseBlobReceipt` 新增可选 expected retention mode，所有 price/charge/settlement/
  invoice/provider/payment/ledger 写入型 publisher 都传入本次请求的 `RETENTION_MODE`，
  read receipt 仍按合法 mode 与 min retain 校验。这样返回合法但非请求策略的
  COMPLIANCE/GOVERNANCE archive receipt 会 fail closed。回归覆盖 immutable archive receipt
  retention mode drift；`go test ./hack/production/internal/meteringbilling -run 'Test.*Receipt|TestPublisher' -count=1`、
  `go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。
- A484 收紧 Operation audit archiver 的 retention receipt 绑定：Object Lock executor
  返回后，archiver 在 release audit finalizer 前重新读取 canonical archive receipt，并要求
  receipt 的 retention mode 与 retain-until 精确等于本次请求。这样合法但非请求策略的
  Operation audit receipt 不会释放 finalizer。回归覆盖 receipt retention mode drift；
  `go test ./hack/production/internal/operationarchiver -run 'TestArchiveProcessor' -count=1`、
  `go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。
- A485 收紧 Operation audit archiver 的归档目的地绑定：release audit finalizer 前，
  archiver 现在还要求 archive receipt 的 object store ID、bucket 和 object key 精确等于
  本次请求。这样 artifact identity 正确但指向错误 bucket/key 的 receipt 不会释放 finalizer。
  回归覆盖 receipt object key drift；`go test ./hack/production/internal/operationarchiver -run 'TestArchiveProcessor' -count=1`、
  `go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。
- A486 将 Operation audit release 的 expected Object Lock scope 下沉到共享 release
  API：`ReleaseWithExpectedReceipt` 会在移除 finalizer 前校验 object store ID、bucket、
  object key、retention mode 和 retain-until；archiver 改用该 API，手工
  `operation-audit --action release` 也必须显式传入这些 expected flags。回归覆盖 release
  expected object drift；`go test ./hack/production/internal/operationauditrelease ./hack/production/internal/operationarchiver -run 'Test.*Release|TestArchiveProcessor' -count=1`、
  `go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。
  随后补齐 `archive-operation-audit.sh` 的 release 参数透传，并在脚本入口拒绝非法
  retention mode/retain-until；脚本级 fake `go` 回归固定 capture、archive、release
  三段调用都绑定同一 expected receipt scope。
  内部 release API 同步移除无 expected scope 的包装入口，`ReleaseWithExpectedReceipt`
  现在对空 scope 直接 fail closed，避免后续生产调用方绕过 Object Lock 目的地绑定。
  `cmd/operation-audit` 的 client config 同步对齐 operationctl：空 kubeconfig 先尝试
  in-cluster ServiceAccount，再回落到标准 kubeconfig；回归覆盖空默认、in-cluster 优先和
  显式 kubeconfig 不触发 in-cluster。
- A487 收紧 object storage usage stdout JSON 解析：sample archiver 读取 file receipt 后，
  stdout 也改用 `DisallowUnknownFields` 和 trailing JSON 拒绝，再逐字段比较两份 receipt。
  这样 stdout 额外未知字段不会被 `json.Unmarshal` 忽略后误判为匹配，也不会触发后续
  archive。回归覆盖 usage stdout unknown field 且 archive 未调用；
  `go test ./hack/production/internal/meteringstorage -run 'TestArchiverRejectsUsage|TestArchiverMeasures' -count=1`、
  `go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。
- A488 收紧 resource metering Prometheus response trailing JSON 校验：collector 保留
  Prometheus 顶层扩展字段兼容，但第一次 decode 后会拒绝响应体中拼接第二个 JSON 值。
  这样合法 vector 后追加的额外 JSON 不会被忽略。回归覆盖 trailing JSON；
  `go test ./hack/production/internal/meteringarchive -run 'TestCollector' -count=1`、
  `go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。
- A489 收紧 backup scheduler parameter template 解析：模板 Secret 现在用 `UseNumber`
  decode，要求顶层为 JSON object 并拒绝 trailing JSON；`null` 模板会 fail closed 且不会
  创建 Operation，大整数模板字段不会经过 float64 重新编码。回归覆盖 null template、
  JSON number 精度保留和 trailing JSON；`go test ./hack/production/internal/backupscheduler -run 'Test.*ParameterTemplate|TestReconcileRejectsNull' -count=1`、
  `go test ./hack/production/... ./deploy/production` 与
  `go vet ./hack/production/... ./deploy/production` 通过。
- A490 收紧 objectstore CLI 环境 JSON 数组解析：`logical-object` 对
  `RECEIPT_INPUTS_JSON`、`ARTIFACT_FORMATS_JSON` 和 `ALLOWED_FORMATS_JSON` 改用共享
  strict parser，要求输入是单一 JSON string array，拒绝 `null`、非字符串元素、空字符串
  和 trailing JSON。空 receipt list 仍可用于生成空 inventory manifest，但不能用 `null`
  模糊表达；usage allowlist 继续由业务层校验非空、排序、唯一。回归覆盖 canonical array、
  空数组、null、object、non-string、empty string 和 trailing JSON；`cd hack/backup/objectstore && go test ./...`、
  `cd hack/backup/objectstore && go vet ./...`、`go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A491 收紧 cold physical restore receipt 解析：`cold-restore-render` 读取
  `kubebrain.cold-physical-snapshot.v2` receipt 时改用 `DisallowUnknownFields` 并拒绝
  trailing JSON；`cold-restore-verify` 的 snapshot/restore receipt chain 同样改为严格单
  JSON 值解析。这样恢复证据合同之外的未知字段或拼接 JSON 不会被 `json.Unmarshal`
  静默忽略后继续渲染清单或发布 semantic verify receipt。回归覆盖 renderer unknown/
  trailing，以及 verifier snapshot unknown、restore trailing；`go test ./hack/backup/cmd/cold-restore-render ./hack/backup/cmd/cold-restore-verify`、
  `go test ./hack/backup/...`、`go vet ./hack/backup/...`、
  `go test ./hack/production/... ./deploy/production`、`go vet ./hack/production/... ./deploy/production`
  与 `git diff --check` 通过。
- A492 收紧 logical backup JSONL 行 schema：`backupfile.OpenVerified` 及 verified reader
  对 header、record、lease 和 footer 行统一使用严格 decoder，拒绝未知字段和同一行内
  trailing JSON；仅 `type` 探测保留 map 读取，真正消费前仍按对应结构体严格解码。
  这样 digest 正确但混入额外字段的 logical backup artifact 不会被 restore/status/verify
  静默接受。回归构造 SHA 匹配但 header/record/lease/footer 含 unknown field 的 JSONL；
  `go test ./hack/backup/internal/backupfile`、`go test ./hack/backup/...`、
  `go vet ./hack/backup/...`、`go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A493 收紧 production JSON array 参数解析：动态 namespace inventory 的
  `namespaces.json` 和 `kubebrain-metering-storage-archive --allowed-formats-json`
  都改为单一 JSON string array parser，拒绝 `null` 与 trailing JSON；namespace
  allowlist 继续统一校验非空、DNS、去重和 256 项上限，storage allowlist 继续由
  archiver 校验非空、排序、唯一。回归覆盖 namespace null/trailing，以及 storage
  canonical/empty/null/object/trailing；`go test ./hack/production/internal/namespaceinventory ./hack/production/cmd/metering-storage-archive`、
  `go test ./hack/production/... ./deploy/production`、`go vet ./hack/production/... ./deploy/production`
  与 `git diff --check` 通过。
- A494 收紧 follower read revision `/status` 响应解析：follower 从 leader 拉取 read
  revision 时改用 `DisallowUnknownFields`，并拒绝 trailing JSON；未知字段、拼接 JSON、
  malformed body 或零 revision 都不会推进本地 read revision。这样代理/LB 错误页、半截
  响应或合同外 status body 不会被静默采纳为可服务的 read fence。回归覆盖 malformed、
  empty object、zero、unknown field 和 trailing JSON；`go test ./pkg/server/service/revision -run 'TestFollowerRejectsMalformedOrZeroRevision|TestRevisionSyncerRejectsResponseAcrossLeaderChange|TestRevisionSyncerRejectsResponseAcrossLeadershipTermChange' -count=20`、
  `go test ./pkg/server/service/revision ./pkg/server/etcd`、
  `go vet ./pkg/server/service/revision ./pkg/server/etcd`、
  `go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A495 收紧 simple auth token claims 解析：HMAC 验签后的 payload 现在仍会用
  `DisallowUnknownFields` 解码，并拒绝 trailing JSON；签名正确但包含合同外字段或拼接
  JSON 的 token 会返回 `ErrInvalidAuthToken`。这样签名只能证明 payload 来自当前 signing
  key，不能让未知 claims 在验证端静默穿透。回归构造重新签名的 unknown-field 和 trailing
  payload；`go test ./pkg/server/etcd -run 'TestAuthToken' -count=20`、
  `go test ./pkg/server/etcd ./pkg/server/service/revision`、
  `go vet ./pkg/server/etcd ./pkg/server/service/revision`、
  `go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A496 收紧 CORRUPT alarm member metadata 解析：TiKV internal key `alarms/corrupt`
  现在只接受单一 JSON `[]uint64`，拒绝 `null`、非数组和 trailing JSON；既有严格递增校验
  继续拒绝重复或降序 member ID。坏 metadata 会让 `CorruptAlarms`/Maintenance health
  读取失败，而不是被解释成空集合后误清除 CORRUPT 状态。回归直接注入 null/object/
  trailing/duplicate/descending metadata；`go test ./pkg/server/etcd -run 'TestCorruptAlarm(MemberSetConcurrentCAS|MetadataRejectsInvalidJSON|BlocksEtcdApplierSurfaceOverGRPC)' -count=20`、
  `go test ./pkg/backend ./pkg/server/etcd`、`go vet ./pkg/backend ./pkg/server/etcd`、
  `go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A497 修复 lease attachment reload 静默丢绑定：`loadLeaseRecords` 读取 legacy
  user-MVCC `leasekeys/<key>` 和新 internal attachment 时，遇到非 int64 lease ID
  现在直接返回错误；旧逻辑 `continue` 或只在 parse 成功时覆盖，会让坏 metadata 在换主/
  重启后被静默跳过，导致 key 在内存索引中丢失 lease 绑定并绕过过期/Revoke 删除。
  回归直接注入 legacy 与 internal 两类坏 attachment value；`go test ./pkg/server/etcd -run 'TestLoadLeaseRecordsRejectsInvalidAttachmentMetadata|TestReloadLeasesAdoptsLeasesGrantedAfterSnapshot|TestLeaseRevokeReclaimsLegacyAndInternalAttachments|TestLeaseCheckpointBoundsReloadAndRenewClearsIt' -count=20`、
  `go test ./pkg/server/etcd ./pkg/backend`、`go vet ./pkg/server/etcd ./pkg/backend`、
  `go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A498 收紧 metering storage archive path-style 布尔环境变量：`SOURCE_S3_FORCE_PATH_STYLE`
  和 `METERING_S3_FORCE_PATH_STYLE` 现在通过共享 optional bool parser 解析，空值仍表示
  false，非空非法值会让 CLI fail closed；旧逻辑忽略 `strconv.ParseBool` 错误，会把拼写
  错误静默降级为 false，可能访问错误的 S3 path style。回归覆盖空值、`true`、`0` 和
  非法值；`go test ./hack/production/cmd/metering-storage-archive -run 'TestParseJSONStringArray|TestParseOptionalBoolEnv' -count=20`、
  `go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A499 收紧 logical objectstore executor `S3_FORCE_PATH_STYLE` 解析：`logical-object`
  现在同样使用 optional bool parser，空值仍表示 false，非空非法值会在创建 S3 client 前
  返回错误；旧逻辑只比较字符串是否等于 `true`，拼写错误会被静默当 false。回归覆盖空值、
  `true`、`0` 和非法值；`cd hack/backup/objectstore && go test ./cmd/logical-object -run 'TestParseJSONStringArray|TestParseOptionalBoolEnv' -count=20`、
  `cd hack/backup/objectstore && go test ./...`、
  `cd hack/backup/objectstore && go vet ./...`、
  `go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A500 收紧 logical restore `ALLOW_OVERWRITE` 布尔解析：空值仍表示 false，明确的
  `true/false/1/0/yes/no` 可用，其他值会在读取 artifact、创建 lease 或写目标 key 前
  fail closed；旧逻辑把任何非 true/1/yes 值都静默当 false，容易掩盖恢复操作参数拼写错误。
  回归覆盖大小写、空值、显式 false 和非法值；`go test ./hack/backup/cmd/logical-restore -run 'TestEnvBool|TestValidateBatchSize|TestRollbackCommittedBatches' -count=20`、
  `go test ./hack/backup/...`、`go vet ./hack/backup/...`、
  `go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A501 修复 JWT auth-token option value 含 `=` 被误拒：`--auth-token=jwt,...`
  解析现在使用 `SplitN(..., 2)`，只按第一个等号分隔 key/value；文件路径或其他值中合法
  包含 `=` 时不再被当作 malformed option。缺少 `=`、空 key、重复 option 和 unsupported
  algorithm 仍 fail closed。回归覆盖 HMAC key 路径中目录名和文件名都含 `=` 的配置；
  `go test ./pkg/server/etcd -run 'TestJWTProvider|TestJWTManager|TestAuthToken' -count=20`、
  `go test ./pkg/server/etcd ./pkg/server/service/revision`、
  `go vet ./pkg/server/etcd ./pkg/server/service/revision`、
  `go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A502 收紧共享 leader election record 解析：`resourceLock.Get` 读取 TiKV 中的
  election metadata 时改用 strict JSON decoder，拒绝未知字段和尾随拼接 JSON；坏选主
  metadata 会阻止本轮锁记录读取，而不是被标准 `json.Unmarshal` 静默降级为已知字段子集。
  回归直接注入 unknown-field 与 trailing JSON 的 election record；`go test ./pkg/backend/election -run TestResourceLockRejectsMalformedElectionMetadata -count=30`、
  `go test ./pkg/backend/election ./pkg/server/service/leader`、
  `go test ./pkg/backend -run 'TestBackend/.*/resource_lock|TestBackend/.*/resource_lock_release_after_renew' -count=1`、
  `go vet ./pkg/backend/election ./pkg/server/service/leader`、
  `go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A503 收紧 lease meta record reload 解析：`loadLeaseRecords` 读取 legacy
  user-MVCC `leases/<id>` 与 internal lease metadata 时改用 strict JSON decoder，拒绝
  未知字段和尾随拼接 JSON；坏 lease meta 会让 reload fail closed，而不是被宽松
  `json.Unmarshal` 静默降级后重建错误的内存 lease 状态。回归覆盖 legacy/internal 两条路径的
  unknown-field 与 trailing JSON，并继续覆盖坏 attachment value；`go test ./pkg/server/etcd -run 'TestLoadLeaseRecordsRejects(MalformedLeaseMetadata|InvalidAttachmentMetadata)|TestLegacyLeaseRecordMigratesToAttachments|TestLeaseRevokeReclaimsLegacyAndInternalAttachments|TestLeaseCheckpointBoundsReloadAndRenewClearsIt' -count=20`、
  `go test ./pkg/server/etcd ./pkg/backend`、`go vet ./pkg/server/etcd ./pkg/backend`、
  `go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A504 收紧 operation parameter broker 必需 query 参数解析：`/v1/parameters` 的
  `namespace`、`name`、`owner` 和 `attempt` 必须各恰好出现一次，重复参数不再被
  `Query().Get` 静默取第一个值。这样 executor 取回动态参数时，代理、审计和服务端不会对
  ambiguous query 产生不同解释；缺失、重复、非法 namespace 或非正 attempt 均 fail closed。
  回归覆盖四个必需参数重复同值时返回 400 且不泄露参数内容；`go test ./hack/production/internal/parameterbroker -run 'TestHandler(ReturnsOnlyCurrentTypeBoundWorkerParameters|RejectsAmbiguousRequiredQueryParameters|FailsClosedForIdentityTypeAudienceAndFencing)' -count=20`、
  `go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A505 修复 `operationctl --action parameters` 对 broker 超大响应的截断成功：客户端仍只允许
  最多 4 MiB 参数响应，但现在读取 `limit+1` 字节并在超限时直接返回错误；旧逻辑用
  `io.LimitReader` 读 4 MiB 后无超限检测，会把截断参数写到 stdout，再依赖 executor digest
  校验兜底。回归覆盖 TLS broker 返回 `max+1` 字节时 fail closed；`go test ./hack/production/cmd/operationctl -run 'TestBrokerParameters|TestDefaultKubeconfig' -count=20`、
  `go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A506 收紧 `operationctl --action parameters` 的 broker endpoint origin 校验：HTTPS
  endpoint 现在除 scheme/host 外还拒绝 userinfo、query 和 fragment，避免把带凭据或歧义
  suffix 的 URL 当成 broker origin 后再追加 `/v1/parameters`。回归覆盖 `http://`、
  `https://user@...`、带 query 与 fragment 的 endpoint 均在读 token/CA 前 fail closed；
  `go test ./hack/production/cmd/operationctl -run 'TestBrokerParameters|TestDefaultKubeconfig' -count=20`、
  `go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A507 收紧 `operationctl --action parameters` 的 projected token 读取：客户端现在最多读取
  `16KiB+1` token 文件内容，空 token 或超过 16 KiB 的 token 会在读 CA/发请求前 fail
  closed；旧逻辑 `os.ReadFile` 无本地上限，可能构造超大 Authorization header，直到
  broker 或 HTTP transport 才失败。回归覆盖空白 token 和 oversized token；`go test ./hack/production/cmd/operationctl -run 'TestBrokerParameters|TestDefaultKubeconfig' -count=20`、
  `go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A508 收紧 `operationctl --action parameters` 的 broker response media type：HTTP 200
  现在还必须携带 `Content-Type: application/json`，否则 fail closed 且不把响应 body 写到
  stdout。旧逻辑只检查 status code，HTML/text 错误页会被当作参数文件输出，再依赖后续
  digest 校验兜底。回归覆盖 TLS broker 返回 200 `text/html`；`go test ./hack/production/cmd/operationctl -run 'TestBrokerParameters|TestDefaultKubeconfig' -count=20`、
  `go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A509 收紧 follower read revision `/status` body 读取：leader `/status` 正常 body 只应是
  小 JSON，现在 OK 与非 OK 响应都通过 4 KiB 上限读取；OK 响应超限会 fail closed，且错误
  归类为可重试 leader 状态问题。旧逻辑 `io.ReadAll` 无上限，并会在 malformed/zero revision
  错误中带全量 body，异常 leader/proxy body 可能拖内存或刷大日志。回归覆盖 OK body 超过
  上限时不返回 revision 且保持 retryable；`go test ./pkg/server/service/revision -run 'TestFollowerRejectsMalformedOrZeroRevision|TestFollowerRejectsOversizedLeaderStatus|TestRevisionSyncerRejectsResponseAcrossLeader(Change|shipTerm)Change' -count=20`、
  `go test ./pkg/server/service/revision ./pkg/server/etcd`、
  `go vet ./pkg/server/service/revision ./pkg/server/etcd`、
  `go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A510 收紧 metering sample collector 的 Prometheus response 读取：仍以 1 MiB 为响应预算，
  但现在读取 `limit+1` 并在超限时 fail closed；旧逻辑 `io.LimitReader` 截断后继续按 JSON
  解析，超大错误页或响应可能被截断写入错误信息或交给后续解析。回归覆盖 Prometheus
  返回超过 1 MiB body；`go test ./hack/production/internal/meteringarchive -run 'TestCollector(BuildsCanonicalCompleteSample|RejectsOversizedPrometheusResponse|FailsClosedOnIncompleteDuplicateStaleAndInvalidValues|AcceptsPrometheusResponseExtensions)' -count=20`、
  `go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A511 收紧 metering billing CSV 导入边界：provider statement 和 payment ledger 不再用
  `csv.ReadAll()` 一次性读取完整外部 CSV，而是通过共享 bounded reader 流式处理，并同时
  限制输入最多 16 MiB、数据行最多 100,000 条。超限输入会在写出 canonical settlement
  artifact 或启动 Object Lock 发布前 fail closed，避免异常账单文件让导入进程无界分配内存。
  回归覆盖 provider/payment 两条路径的 oversized input 与超行数输入；`go test ./hack/production/internal/meteringbilling -run 'TestBuild(ProviderStatement|PaymentLedger)FromCSV|TestPaymentLedgerRejects|TestProviderStatementPublisher' -count=5`、
  `go test ./hack/production/internal/meteringbilling`、`go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A512 收紧 Operation API 的 OIDC/JWKS media type 校验：discovery 与 JWKS 200 响应现在必须
  声明 JSON media type，允许 `application/json` 和 `application/jwk-set+json` 等 `+json`
  类型；`text/html`/`text/plain` 错误页即使 body 恰好是 JSON 也会在读取和解析前 fail
  closed。回归覆盖 discovery 与 JWKS 分别返回非 JSON Content-Type；
  `go test ./hack/production/internal/operationapi -run 'TestOIDCAuthenticator|TestHTTPSAPIAuthenticatesOIDCTokenAndSubmitsOperation|TestHandlerDependencyDeadlineReturnsServiceUnavailable' -count=20`、
  `go test ./hack/production/internal/operationapi`、`go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A513 收紧 metering sample collector 的 Prometheus success media type：非 200 诊断 body
  仍按 A510 的 1 MiB 上限读取，但 HTTP 200 响应现在必须声明 JSON media type；`text/html`
  错误页即使 body 恰好是 Prometheus JSON 也不会进入计量解析。回归覆盖 200 `text/html`
  body；`go test ./hack/production/internal/meteringarchive -run 'TestCollector(BuildsCanonicalCompleteSample|RejectsNonJSONPrometheusSuccessResponse|RejectsOversizedPrometheusResponse|FailsClosedOnIncompleteDuplicateStaleAndInvalidValues|AcceptsPrometheusResponseExtensions)|TestCollectorV3' -count=20`、
  `go test ./hack/production/internal/meteringarchive`、`go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A514 收紧 operation audit 与 objectstore 本地 canonical JSON 读取边界：operation audit
  artifact/archive receipt、backup/deletion/audit/blob/usage/inventory receipt、inventory
  manifest 以及 inventory manifest builder 的 receipt header sniff 现在最多接受 1 MiB，
  超限时在 JSON 解码、canonical 校验、Object Lock 上传或 finalizer release 前 fail closed。
  `ArchiveAudit` 改为上传 `operationaudit.InspectBytes` 返回的已验证 frozen bytes，远端复核
  阶段也重新有界读取本地 artifact，避免旧 `os.ReadFile` 路径在 TOCTOU 或异常大文件下
  无界分配。回归覆盖 oversized artifact 在触发 S3 前失败、operationaudit reader 和
  objectstore JSON reader 的 oversized 输入；
  `go test ./hack/production/operationaudit -run 'TestOperationAudit' -count=20`、
  `(cd hack/backup/objectstore && go test ./internal/objectstore -run 'TestArchiveAudit|TestObjectStoreJSONReaders|TestReceiptReaders|TestInventoryManifest|TestBuildInventoryManifest|TestMeasureUsage|TestUsageReceipt|TestArchiveBlob' -count=20)`、
  `go test ./hack/production/... ./deploy/production`、
  `(cd hack/backup/objectstore && go test ./...)`、
  `go vet ./hack/production/... ./deploy/production`、
  `(cd hack/backup/objectstore && go vet ./...)` 与 `git diff --check` 通过。
- A515 修复 cold restore renderer/verifier 的 strict receipt schema 漂移：真实
  `cold-snapshot-execute.sh` 生成的 `kubebrain.cold-physical-snapshot.v2` receipt 带顶层
  `created_at`、完整 preflight inventory、snapshot name/UID/content identity，以及
  `semantic_witness.leases`；`cold-restore-render` 之前缺这些字段，strict decoder 会把生产
  snapshot receipt 拒成 unknown field，导致隔离恢复清单无法渲染。现在 renderer 与 restore
  verifier 都显式接受这些字段，并要求 `created_at` 为 RFC3339、inventory format 为
  `kubebrain.cold-physical-snapshot-preflight.v2`，继续拒绝真正未知字段和尾随 JSON。回归覆盖
  invalid `created_at`/inventory format 和 production cold restore execute smoke；
  `go test ./cmd/cold-restore-render ./cmd/cold-restore-verify -count=1`（在
  `hack/backup` 模块内）、`go test ./hack/production -run TestColdRestoreExecute -count=1`、
  `go test ./hack/production/... ./deploy/production`、`(cd hack/backup && go test ./cmd/cold-restore-render ./cmd/cold-restore-verify)`、
  `go vet ./hack/production/... ./deploy/production`、`(cd hack/backup && go vet ./cmd/cold-restore-render ./cmd/cold-restore-verify)`、
  与 `git diff --check` 通过。
- A516 收紧 cold restore 小型 JSON 输入读取边界：`cold-restore-render` 的 snapshot
  receipt，以及 `cold-restore-verify` 的 snapshot receipt、restore receipt 和 rendered
  restore manifest 现在最多读取 4 MiB+1 并在超过 4 MiB 时 fail closed，避免异常本地
  JSON 证据在 strict schema 解码、manifest canonical 比对或语义验证前无界分配。逻辑
  witness 文件可能随实例 keyspace 放大，本次不套用小型 JSON 上限，仍由 backupfile parser
  和 digest 门禁验证。回归覆盖 renderer/verifier bounded reader 的 oversized 输入；
  `go test ./cmd/cold-restore-render ./cmd/cold-restore-verify -run 'Test(ReadBoundedJSONFile|RenderColdRestoreManifest|DecodeReceipt|ValidateReceiptChain|ValidateRestoreManifestBinding)' -count=20`（在
  `hack/backup` 模块内）、`go test ./cmd/cold-restore-render ./cmd/cold-restore-verify`、
  `go test ./hack/production -run TestColdRestoreExecute -count=5`、
  `go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production`、
  `(cd hack/backup && go vet ./cmd/cold-restore-render ./cmd/cold-restore-verify)` 与
  `git diff --check` 通过。
- A517 收紧 `kubebrain-metering-archive` 的 Prometheus credential 文件读取边界：
  `--prometheus-ca-file` 现在最多读取 1 MiB，`--prometheus-bearer-token-file` 最多读取
  16 KiB，超限时在 TLS 初始化、collector 创建和 HTTP 请求前 fail closed。旧逻辑直接
  `os.ReadFile`，异常 Secret/ConfigMap 投影可能造成无界本地分配或构造超大
  Authorization header。回归覆盖 oversized token 与 oversized CA；
  `go test ./hack/production/cmd/metering-archive -run 'Test(ReadToken|PrometheusClient)' -count=20`、
  `go test ./hack/production/cmd/metering-archive`、`go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A518 收紧 `operationctl --action parameters` 的 broker CA 文件读取边界：parameter
  broker token、endpoint、response size 和 response media type 已在 A505-A508 收紧，但
  `--parameters-ca-file` 仍直接 `os.ReadFile`。现在 CA bundle 最多读取 1 MiB，超限会在
  x509 pool 初始化和 broker HTTP 请求前 fail closed。回归覆盖 valid token + oversized CA；
  `go test ./hack/production/cmd/operationctl -run 'TestBrokerParameters|TestDefaultKubeconfig' -count=20`、
  `go test ./hack/production/cmd/operationctl`、`go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A519 收紧 metering price catalog 本地 JSON 读取边界：approved price catalog 现在最多
  读取 1 MiB，超限会在 strict schema 解码、canonical 比对和 price publisher Object Lock
  executor 启动前 fail closed。`writeCanonicalAtomic` 比较已有文件时也只读取目标 canonical
  长度加 1 字节，避免为了判断幂等而无界读取错误的大文件，同时不把 provider/payment 等
  可能较大的 settlement artifact 强行套 1 MiB 上限。回归覆盖 oversized catalog read 和
  oversized existing output；`go test ./hack/production/internal/meteringbilling -run 'TestCatalogAndChargeReject|TestPriceCatalogRejects|TestBuildCharge|TestPublisher' -count=20`、
  `go test ./hack/production/internal/meteringbilling`、`go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A520 收紧 object storage metering 小型 JSON 本地读取边界：object usage receipt、hourly
  sample 和 daily rollup 现在最多读取 1 MiB，超限会在 strict schema 解码、canonical
  比对、日汇总构造或 Object Lock 归档前 fail closed。sample/rollup atomic writer 比较
  已有文件时也只读取目标 canonical 长度加 1 字节；Roller 改用 `ReadSnapshot` 已验证的
  bytes/digest 与 blob-read receipt 比对，不再为了校验 receipt 第二次无界读取 sample
  文件。回归覆盖 oversized usage receipt、sample、rollup 和 oversized existing output；
  `go test ./hack/production/internal/meteringstorage -run 'Test(Snapshot|StorageRollup|UsageReceipt|Archiver|ValidateObjectStorage|ParseObjectStorage)' -count=20`、
  `go test ./hack/production/internal/meteringstorage`、`go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A521 收紧 core metering sample/rollup 本地 JSON 读取边界：metering sample 和 daily
  rollup 现在最多读取 1 MiB，超限会在 strict schema 解码、canonical 比对、日汇总构造
  或 Object Lock executor 启动前 fail closed。sample/rollup atomic writer 比较已有文件
  时也只读取目标 canonical 长度加 1 字节；Roller 改用 `ReadSampleStatus` 已验证的
  bytes/digest 与 blob-read receipt 比对，不再为了校验 receipt 第二次无界读取 sample
  文件。回归覆盖 oversized sample、rollup 和 oversized existing output；
  `go test ./hack/production/internal/meteringarchive -run 'Test(Collector|Sample|BuildRollup|Rollup|Roller|ParseMetering)' -count=20`、
  `go test ./hack/production/internal/meteringarchive`、`go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A522 收紧生产子进程 captured output 边界：新增 `processgroup.CombinedOutput`，与
  `exec.Cmd.CombinedOutput` 一样合并 stdout/stderr，但最多保留 1 MiB；超限会调用
  `CommandContext`/`processgroup.Configure` 的 cancel，终止 executor 进程组并返回
  `process output exceeds ...`。operation archiver、core metering archiver、object storage
  metering archiver 和 metering biller 的本地 executor 调用全部改用该 helper，避免异常
  子进程通过无界 stdout/stderr 让 wrapper 分配内存。回归覆盖正常合并输出、非零退出错误
  保留和超限取消；
  `go test ./hack/production/internal/processgroup -count=20`、
  `go test ./hack/production/internal/processgroup ./hack/production/internal/operationarchiver ./hack/production/internal/meteringarchive ./hack/production/internal/meteringbilling ./hack/production/internal/meteringstorage`、
  `go test ./hack/production/... ./deploy/production`、`go vet ./hack/production/... ./deploy/production`
  与 `git diff --check` 通过。
- A523 收紧 objectstore immutable blob 本地 output 幂等比较边界：`blob-read` 下载远端 exact
  version 后，本地 output 若已存在，现在只读取远端 metadata/digest 已验证的期望对象大小加
  1 字节；超过该大小会 fail closed，而不是为了判断是否可幂等复用而 `os.ReadFile` 无界读取
  错误的大文件。artifact payload 的业务上限仍为 16 MiB，小型 receipt/manifest 仍沿用 1 MiB
  JSON 预算。回归覆盖 oversized existing output；
  `(cd hack/backup/objectstore && go test ./internal/objectstore -run 'Test(ReadBlob|ArchiveBlob)' -count=20)`、
  `(cd hack/backup/objectstore && go test ./...)`、`go test ./hack/production/... ./deploy/production`、
  `(cd hack/backup/objectstore && go vet ./...)`、`go vet ./hack/production/... ./deploy/production`
  与 `git diff --check` 通过。
- A524 收紧 payment ledger 离线 invoice source 本地 JSON 读取边界：`kubebrain-metering-payment-ledger`
  的离线 `--invoice-source` 是小型 source binding JSON，现在最多读取 1 MiB；超限会在 strict
  schema 解码、invoice/source SHA/bytes 绑定校验和 ledger 写出前 fail closed。生产 exact-read
  路径仍由 Object Lock read receipt 生成 source，不受该离线文件预算影响。回归覆盖 oversized
  invoice source；
  `go test ./hack/production/cmd/metering-payment-ledger -run TestReadSource -count=20`、
  `go test ./hack/production/cmd/metering-payment-ledger`、`go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A525 收紧 metering Biller exact-read 本地 artifact 校验边界：Biller 读取 Object Lock
  exact-read 下载到本地的 rollup、price catalog 和 object storage rollup 时，现在只按
  blob-read receipt 的 `object_bytes + 1` 有界读取并复算 digest；超限会在 charge 构造前
  fail closed，不再为了校验 receipt 而 `os.ReadFile` 无界读取异常大的本地文件。新增
  `sourceFromDownloadedArtifact` helper，后续 provider/payment/general-ledger exact-read 路径可
  继续复用。回归覆盖 oversized downloaded artifact；
  `go test ./hack/production/internal/meteringbilling -run 'TestBiller' -count=20`、
  `go test ./hack/production/internal/meteringbilling`、`go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A526 复用 Biller 的 bounded exact-read source helper 覆盖剩余 billing source 路径：
  invoice finalizer、invoice number assignment、provider reconciliation、payment ledger 和
  general ledger export 现在都通过 `sourceFromDownloadedArtifact` 按 read receipt 的
  `object_bytes + 1` 有界读取本地下载文件并复算 digest；异常大文件会在构造下游
  canonical artifact 前 fail closed。这样 billing exact-read 输入校验不再分散保留多份
  `os.ReadFile` 逻辑。回归沿用 A525 oversized helper 负例并覆盖各路径成功流；
  `go test ./hack/production/internal/meteringbilling`、`go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A527 收紧 metering charge 本地 canonical JSON 读取边界：`ReadCharge` 现在最多读取
  1 MiB，超限会在 invoice plan/final invoice 的 strict schema 解码、canonical 比对和
  source 绑定校验前 fail closed。charge artifact 本身是固定 catalog/rollup/source
  摘要与少量 line item 的小型 canonical JSON；provider/payment/general-ledger 等可能随
  交易规模放大的 settlement artifact 不套该上限。回归覆盖 oversized charge read；
  `go test ./hack/production/internal/meteringbilling -run 'Test(BuildCharge|ChargeRejects|CatalogAndChargeReject|PriceCatalogRejects)' -count=20`、
  `go test ./hack/production/internal/meteringbilling`、`go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A528 收紧 settlement 控制证据本地读取边界：adjustment、invoice plan 和 finalized
  invoice 是周期级控制证据，`readSettlement` 现在最多读取 4 MiB；`DecodeSettlementKind`
  的 kind sniff 也使用同一有界读取。超限会在 strict schema 解码、canonical 比对、
  kind 分发或 Object Lock 发布前 fail closed。provider statement、payment ledger 和
  general ledger export 可能随交易行数放大，不套该 4 MiB 控制证据预算，仍由各自 CSV/
  生成路径和 exact-read digest 门禁约束。回归覆盖 oversized settlement read 和 kind sniff；
  `go test ./hack/production/internal/meteringbilling -run 'Test(Settlement|BuildInvoice|InvoicePlan|InvoiceFinalizer)' -count=20`、
  `go test ./hack/production/internal/meteringbilling`、`go test ./hack/production/... ./deploy/production`、
  `go vet ./hack/production/... ./deploy/production` 与 `git diff --check` 通过。
- A529 扩展 client/v3 批量 lease renewal Porcupine 模型：对照
  `/root/etcd/tests/common/lease_test.go` 的 `TestLeaseGrantAndList`、
  `TestLeaseGrantTimeToLiveExpired` 和 `TestLeaseGrantKeepAliveOnce`，新增
  `TestClientV3LeaseBatchRenewalHistoryIsLinearizable`。该黑盒历史同时创建 6 个短
  TTL lease，只对偶数 lease 持续 `KeepAliveOnce`，通过 watch 记录奇数 lease 的自然
  删除，并最终验证 `TimeToLive(WithAttachedKeys)`、`Get` 和 `Leases` 只保留被续约
  集合。配套 `TestLeaseBatchRenewalModelRequiresOriginalDeadlineIsolation` 明确拒绝
  两类反例：已续约 lease 被原始 deadline 删除、未续约 lease 过期后仍能读到旧 key。
  无端点时 live 历史按既有 client/v3 兼容测试约定跳过；模型单测和编译门禁通过；
  `(cd hack/etcd-client-compat && go test . -run 'TestLeaseBatchRenewalModelRequiresOriginalDeadlineIsolation|TestClientV3LeaseBatchRenewalHistoryIsLinearizable' -count=20)`、
  `(cd hack/etcd-client-compat && go test . -run 'TestLeaseBatchRenewalModelRequiresOriginalDeadlineIsolation|TestClientV3LeaseBatchRenewalHistoryIsLinearizable' -race -count=1)`
  与 `git diff --check` 通过。
- A530 收紧 JWT auth token provider key 文件启动读取边界：对照
  `/root/etcd/server/etcdserver/server.go` 中 `auth.NewTokenProvider` 的启动配置路径和
  `/root/etcd/tests/framework/integration/cluster.go` 的 JWT fixture，KubeBrain 继续支持
  `jwt,sign-method=...,pub-key=...,priv-key=...,ttl=...` 语法与 HMAC/RSA/PSS/ECDSA/Ed25519
  行为，但 `pub-key`/`priv-key` 文件现在最多读取 1 MiB。异常大 secret 会在 PEM/JWT
  解析和服务启动前 fail closed，错误保留 `read JWT <option>` 前缀；正常 PEM 和 HMAC
  secret 不受影响。回归覆盖 oversized private/public key；
  `go test ./pkg/server/etcd -run 'TestJWTProvider' -count=20`、
  `go test ./pkg/server/etcd -run 'TestJWT|TestAuthTokenUsesConfiguredTTL|TestAuthTokenRejectsWhileDisabled' -count=5`、
  `go test ./pkg/server/etcd -count=1`、`go vet ./pkg/server/etcd` 与
  `git diff --check` 通过。
- A531 收紧 endpoint TLS mounted secret 动态重载读取边界：对照
  `/root/etcd/tests/framework/integration/cluster.go` 中通过 `GetCertificate` 触发
  `--cert-file/--key-file/--trusted-ca-file` 动态加载的 TLS 路径，KubeBrain 仍保持每次
  握手重载 cert/key/CA/CRL 的轮换语义，但显式用 bounded read 后再调用
  `tls.X509KeyPair`/x509 解析。cert/key/CA PEM 最多 1 MiB，CRL 最多 16 MiB；启动校验
  和后续握手 reload 都会在 oversized mounted secret/configmap 前 fail closed，避免异常
  文件在 TLS 握手路径无界分配。回归覆盖 oversized serving cert/key、distinct outbound
  client cert、trusted CA、CRL，以及轮换后 oversized cert/CA/CRL；
  `go test ./pkg/endpoint -run 'Test(SecurityConfigRejectsOversizedMountedTLSFiles|TLSReloadRejectsOversizedRotatedFiles|ServerCertificateReloadedForEveryHandshake|InboundClientCATrustPoolRotation|CertificateRevocationListReloadedForEveryHandshake)' -count=10`、
  `go test ./pkg/endpoint -run 'Test(SecurityConfigRejectsOversizedMountedTLSFiles|TLSReloadRejectsOversizedRotatedFiles)' -race -count=1`、
  `go test ./pkg/endpoint -count=1`、`go vet ./pkg/endpoint` 与 `git diff --check`
  通过。
- A532 收紧 production operation HTTPS 证书重载读取边界：A270/A271 已让 parameter
  broker 与 operation API 在线重载 Kubernetes TLS Secret，但共用的
  `hack/production/internal/tlscertreload` 仍通过 `tls.LoadX509KeyPair` 无界读取
  `--tls-cert-file/--tls-key-file`。现在该 helper 与 endpoint TLS 路径一致，先分别按
  1 MiB 上限读取 cert/key，再调用 `tls.X509KeyPair`；oversized 轮换会 fail closed 并继续
  保留上一份有效证书，不会污染 readiness 或 HTTPS serving。回归覆盖 oversized cert/key
  后 current serial 不变；
  `go test ./hack/production/internal/tlscertreload -count=20`、
  `go test ./hack/production/internal/tlscertreload ./hack/production/cmd/operation-api ./hack/production/cmd/operation-parameter-broker`、
  `go test ./hack/production/... ./deploy/production`、`go vet ./hack/production/... ./deploy/production`
  与 `git diff --check` 通过。
- A533 将 cold restore semantic witness 文件绑定改为流式 SHA-256：A516 正确没有给
  逻辑 witness 套 4 MiB 小型 JSON 上限，因为 witness 会随实例 keyspace 放大；但
  `cold-restore-verify` 为校验 snapshot receipt 的 `semantic_witness.file_sha256` 仍
  `os.ReadFile` 整个 witness。现在 verifier 继续通过 `backupfile.OpenVerified`/
  `loadWitness` 流式解析 witness 内容，并用 `io.Copy` 计算整文件 SHA-256，不再把
  witness 全量留在内存。receipt chain 语义不变，tampered file SHA 仍 fail closed。
  回归覆盖 `fileDigest` 与 witness binding mismatch；
  `(cd hack/backup && go test ./cmd/cold-restore-verify -run 'Test(FileDigest|ValidateReceiptChain)' -count=20)`、
  `(cd hack/backup && go test ./cmd/cold-restore-render ./cmd/cold-restore-verify)`、
  `(cd hack/backup && go vet ./cmd/cold-restore-render ./cmd/cold-restore-verify)`、
  `(cd hack/backup/objectstore && go test ./...)`、`(cd hack/backup/objectstore && go vet ./...)`
  与 `git diff --check` 通过。
- A534 收紧生产审计/备份 etcd 客户端 TLS 输入边界：`hack/internal/etcdutil` 被
  logical backup/export/restore/verify、cold restore verify、prefix-tool 与
  `hack/production/cmd/etcd-audit-probe` 共用，但其 `TLSConfigFromEnv` 仍通过
  `tls.LoadX509KeyPair` 和 `os.ReadFile` 无界读取 `CERT`/`KEY`/`CACERT` 等环境变量
  指向的 PEM 文件。现在 cert、key、CA bundle 都先按 1 MiB 上限读取，再调用
  `tls.X509KeyPair` 或构造 RootCAs；异常大的 Secret/ConfigMap 投影会在解析前 fail
  closed。回归覆盖 oversized cert/key/CA；
  `go test ./hack/internal/etcdutil -run 'Test(TLSConfigFromEnv|TimeoutFromEnv)' -count=20`、
  `go test ./hack/internal/etcdutil -run 'TestTLSConfigFromEnvRejectsOversizedPEMFiles' -race -count=1`、
  `go test ./hack/internal/etcdutil ./hack/backup/cmd/logical-export ./hack/backup/cmd/logical-restore ./hack/backup/cmd/logical-verify ./hack/backup/cmd/prefix-tool ./hack/backup/cmd/cold-restore-verify ./hack/production/cmd/etcd-audit-probe`、
  `go vet ./hack/internal/etcdutil ./hack/backup/cmd/logical-export ./hack/backup/cmd/logical-restore ./hack/backup/cmd/logical-verify ./hack/backup/cmd/prefix-tool ./hack/backup/cmd/cold-restore-verify ./hack/production/cmd/etcd-audit-probe`、
  `go test ./hack/backup/... ./hack/production/cmd/etcd-audit-probe` 与 `git diff --check`
  通过。
- A535 建立小时级 lease renewal 线性化模型：A529 已覆盖批量 lease 在原 grant
  deadline 处的 renewed/unrenewed 隔离，但 P2 仍缺一个可表达“多次 renewal 后 deadline
  必须反复向后滚动”的长周期模型。新增 `leaseLongRenewalModel`，用虚拟 renewal window
  表达 grant、put、keepalive、time advance、read 和 TTL；模型接受多窗口内持续 keepalive
  后 key/lease 仍存活，拒绝从原 grant deadline 累计耗尽、以及完整窗口过期后才
  keepalive 的历史。live 测试在设置 `KUBEBRAIN_ETCD_ENDPOINT` 时用压缩时间跨越多次原始
  deadline，生成 client/v3 history 并交给 Porcupine 校验。回归：
  `(cd hack/etcd-client-compat && go test . -run 'TestLeaseLongRenewalModelExtendsDeadlineAcrossRenewalWindows' -count=20)`、
  `(cd hack/etcd-client-compat && go test . -run 'Test(ClientV3LeaseRepeatedRenewalHistoryIsLinearizable|LeaseLongRenewalModelExtendsDeadlineAcrossRenewalWindows)' -count=10)`、
  `(cd hack/etcd-client-compat && go test . -run '^$')`、`(cd hack/etcd-client-compat && go vet .)`
  与 `git diff --check` 通过。
- A536 收紧 operation runner 的 namespace 重绑定：Backup、BackupDeletion、
  CertificateRotation、Destroy、PostRestoreAudit 和 RestoreCutover runner 在 claim
  响应携带 `namespace` 时，旧逻辑把新的 `--namespace` 追加到已有 kube args 后面，形成
  `--namespace ops --namespace tenant-a-operations`。多数 Cobra/pflag 调用会以后者为准，
  但审计日志、包装器和未来参数解析会看到两个管理 namespace，削弱 takeover/fencing
  证据的确定性。现在 6 个 runner 都通过 `build_kube_args` 在初始和 namespace 重绑定后
  重建参数数组，并在 `set -euo pipefail` 下显式 `return 0`；回归让 fake claim 返回
  tenant namespace，并断言 succeed/heartbeat 等后续调用只有单个 namespace。回归：
  `go test ./hack/production -run 'Test(BackupOperation|BackupDeletionOperation|CertificateRotationOperation|DestroyOperation|PostRestoreAuditOperation|RestoreCutoverOperation)' -count=1 -timeout=180s`、
  `go test ./hack/production -run 'Test(BackupOperationCompletesProtectedUpload|BackupDeletionOperationCompletesThreeGatesAndRetries|CertificateRotationOperationCompletesLifecycle|DestroyOperationCompletesLifecycle|PostRestoreAuditOperationCompletesAndBindsReceipt|RestoreCutoverOperationCompletesAllPhases)' -count=3 -timeout=120s`
  与 `git diff --check` 通过。
- A537 收紧 destroy/certificate rotation 幂等 receipt 校验：`destroy-instance.sh`
  与 `validate-certificate-rotation.sh` 旧逻辑用 `grep`/行数判断已有 receipt 是否可复用，
  合法 JSON 中的嵌套 shadow 字段也可能包含所有期望文本片段，导致顶层 format 或
  成功布尔值不匹配的 receipt 被误当成同一完成证据。现在两个脚本在 `complete` 阶段通过
  `jq` 生成 receipt，并要求已有 receipt 的顶层字段集合、字段类型和字段值全部精确匹配；
  未知字段、嵌套伪造字段、尾随 JSON 或非正完成时间都会 fail closed。回归覆盖嵌套
  shadow receipt 被拒绝且原文件不被覆盖；
  `go test ./hack/production -run 'Test(DestroyInstance|ValidateCertificateRotation)' -count=1 -timeout=120s`
  与 `git diff --check` 通过。
- A538 收紧剩余生产 operation receipt 复用 schema：cleanup boundary、restore cutover、
  post-restore audit 和 backup deletion operation 已经使用 `jq`，但旧复用校验只检查核心
  字段子集，带未知字段或遗漏次要绑定字段的已有 receipt 仍可能通过并进入 Succeeded
  digest。现在 `cleanup-instance-boundaries.sh`、`switch-restore-traffic.sh`、
  `audit-restored-instance.sh` 和 `run-backup-deletion-operation.sh` 都要求顶层字段集合、
  字段类型、时间戳和所有身份/digest/namespace/secret/sample 绑定精确匹配；cutover 与
  audit receipt 生成也改为 compact sorted JSON。backup deletion 的并发 hard-link
  冲突不再只做逐字节 `cmp`，而是复用同一 strict schema 校验既有 receipt。回归覆盖
  四条路径中带未知字段的 existing receipt 被拒绝且原文件不被覆盖；
  `go test ./hack/production -run 'Test(BoundaryCleanup|PostRestoreAudit|RestoreTrafficCutover|BackupDeletionOperation)' -count=1 -timeout=180s`
  与 `git diff --check` 通过。
- A539 收紧 BackupDeletion runner 输入 receipt schema：A538 关闭了最终 operation
  receipt 复用问题，但 `run-backup-deletion-operation.sh` 对 source backup receipt、
  pre/post inventory receipt 和 deletion receipt 仍只校验字段子集；带未知字段、弱
  时间戳类型或 retention mode 漂移的输入证据可能进入最终 operation digest。现在 runner
  要求 source receipt、inventory receipts 和 deletion receipt 的顶层字段集合、数值类型、
  retention mode/retain-until、manifest SHA、exact version 身份和 absence 证明全部精确
  匹配。回归覆盖 pre inventory、deletion 和 post inventory receipt 带未知字段均被拒绝；
  `go test ./hack/production -run 'TestBackupDeletionOperation' -count=1 -timeout=120s`
  与 `git diff --check` 通过。
- A540 收紧跨 operation 输入 receipt schema：边界清理读取的 destroy receipt、restore
  cutover 读取的 restore-verification receipt，以及 post-restore audit 读取的 cutover
  receipt 仍只做字段子集校验；上游 receipt 带 unknown 字段、弱正数类型或缺少真实
  cutover schema 中的 replicas 时可能继续流入下游证据。现在
  `cleanup-instance-boundaries.sh`、`switch-restore-traffic.sh` 和
  `audit-restored-instance.sh` 都要求输入 receipt 的 exact key set、字段类型、正数时间戳
  和跨操作 identity/digest/prefix 绑定匹配；restore verification receipt 兼容合法的
  `artifact_created_at_unix,omitempty`。回归覆盖三条路径的上游 receipt unknown field；
  `go test ./hack/production -run 'Test(BoundaryCleanup|PostRestoreAudit|RestoreTrafficCutover)' -count=1 -timeout=160s`
  与 `git diff --check` 通过。
- A541 修复本地地址枚举失败 panic：`pkg/util.GetHost` 调用的 `getLocalIPs` 已声明返回
  error，但 `net.Interfaces()` 失败时旧逻辑直接 `panic`，会绕过调用方已有的错误日志和
  空 host fail-closed 路径。现在接口枚举错误按 error 返回；新增测试通过注入失败的
  interface provider 覆盖该路径。回归：
  `go test ./pkg/util -run 'Test(GetLocalIPsReturnsInterfaceError|GetHost)' -count=20`、
  `go test ./pkg/util ./pkg/endpoint ./pkg/backend ./pkg/server/etcd ./pkg/server/service/etcdproxy`
  与 `go vet ./pkg/util ./pkg/endpoint ./pkg/backend ./pkg/server/etcd ./pkg/server/service/etcdproxy`
  通过。
- A542 收紧 operation queue 本地准入校验：`KubeBrainOperation` CRD 已用 CEL/OpenAPI
  约束 operation type、parameters SHA-256 和成功 receipt SHA-256，但
  `hack/production/internal/operationqueue` 的 fake-client/旧集群路径仍只检查 digest
  长度和非空 type。现在 `Queue.Submit` 在 Go 层拒绝 CRD enum 外的 operation type，
  并要求 parameters digest 为小写 hex SHA-256；`Queue.Finish` 对成功 receipt digest
  使用同一小写 hex 校验，不再只接受任意 64 字符串。回归覆盖 unsupported type、
  非 hex parameters digest 和大写 receipt digest；
  `go test ./hack/production/internal/operationqueue -run 'TestQueueRejectsSpecDriftAndInvalidCompletion|TestQueueLifecycleAndExpiredLeaseFencing|TestFinishStatusReconciliationOutlivesCanceledParent' -count=20`
  通过。
- A543 收紧 operation approval 本地身份校验：admission policy 要求只有
  `system:serviceaccount:kubebrain-operations:kubebrain-operation-approver` 能写入
  approval annotations，且 Claim 只承认该 identity；但 `Queue.Approve` 旧逻辑允许
  调用方传入任意非空 `approvedBy`，在 fake-client/旧集群路径可能写入不可 claim 的
  不可变审批证据。现在 Approve 入口直接拒绝非专用 approver identity，并继续要求
  approval ID 为 DNS-compatible；回归覆盖非专用 approver 被拒绝；
  `go test ./hack/production/internal/operationqueue -run 'TestQueue(RequiresApprovalForHighRiskOperations|DoesNotTreatForgedApprovalAsApproved|ApprovalIsPendingOnlyAndIdempotent|RejectsApprovalForLowRiskOrInvalidDecision)|TestApprove(ReconcilesCommittedUpdateAfterLostResponse|ReportsWriteAndReconciliationFailures|RejectsReplacementUIDAfterFailedUpdate)' -count=20`
  通过。
- A544 统一 operation audit digest 为小写 hex：operation queue 与 CRD 都要求
  parameters/receipt SHA-256 为小写 hex，但 `operationaudit.validSHA256` 旧实现通过
  `hex.DecodeString` 校验，仍接受大写 hex。现在 operation audit artifact 与 object
  archive receipt 的 digest 校验都逐字节要求 `[0-9a-f]`，避免 fake-client、离线归档或
  旧集群路径生成与 CRD schema 不一致的审计证据。回归覆盖 artifact parameters/receipt
  大写 digest 和 archive receipt artifact digest 被拒绝；
  `go test ./hack/production/operationaudit ./hack/production/internal/operationauditbuilder ./hack/production/internal/operationauditrelease -run 'Test(OperationAudit|ArchiveReceipt|FromOperation|Release)' -count=20`
  通过。
- A545 收紧 objectstore receipt digest schema：`hack/backup/objectstore/internal/objectstore`
  的 `validHexSHA256` 同样通过 `hex.DecodeString`，会接受大写 hex；backup receipt 和
  deletion receipt 还只要求 `artifact_sha256` 非空，测试 fixture 甚至使用 `"sha"`。
  现在 backup/deletion/audit/blob/read/inventory/usage 共用的小型 receipt 校验都只接受
  小写 `[0-9a-f]{64}`，backup/deletion receipt 也要求 artifact digest 为真实 SHA-256。
  回归覆盖 backup、deletion、audit、blob 和 blob-read receipt 的大写 digest 被拒绝；
  `(cd hack/backup/objectstore && go test ./internal/objectstore -run 'Test(ReceiptsRejectNonLowercaseArtifactDigest|WriteReceiptAtomicIsIdempotentAndNonOverwriting|ReceiptReadersRejectAmbiguousJSON|DeletionReceiptRequiresDeterministicRetentionBoundary|Upload|Delete)' -count=20)`
  通过。
- A546 收紧 cold restore verifier 的 manifest digest schema：`cold-restore-verify`
  的 `validDigest` 仍使用 `hex.DecodeString`，会接受 restore receipt 中大写的
  `restore_manifest.sha256`，与 executor `sha256sum` 输出和其他 receipt schema 不一致。
  现在 verifier 只接受小写 `[0-9a-f]{64}`；回归覆盖大写 manifest digest 被拒绝；
  `go test ./hack/backup/cmd/cold-restore-verify -run 'TestValidateReceiptChain|TestValidateRestoreManifestBinding|TestFileDigest' -count=20`
  通过。
- A547 收紧 cold restore renderer 的 witness digest schema：`cold-restore-render`
  读取 `kubebrain.cold-physical-snapshot.v2` 时对
  `semantic_witness.sha256/file_sha256` 仍只检查长度 64，可能接受与 executor/verifier
  不一致的大写或非 hex 字符串。现在 renderer 也要求小写 `[0-9a-f]{64}`；回归覆盖
  大写 witness digest 被拒绝；
  `go test ./hack/backup/cmd/cold-restore-render -run 'TestRenderColdRestoreManifest|TestRenderColdRestoreManifestRejectsIncompleteReceipts|TestDecodeReceiptRejectsUnknownAndTrailingJSON' -count=20`
  通过。
- A548 收紧 restore cutover 输入 receipt digest schema：`switch-restore-traffic.sh`
  读取 `kubebrain.restore-verification.v1` 时仍只要求 `artifact_sha256` 为非空字符串，
  可能把短 digest、大写 hex 或非 hex 字符串写入 cutover state 与最终
  `kubebrain.restore-cutover.receipt.v1`。现在 prepare 阶段和 receipt parser 都要求
  小写 `[a-f0-9]{64}`；fixture 改用真实 SHA-256 形状，并覆盖短 digest、大写 digest
  和非 hex digest 被拒绝且不生成 state。回归：
  `go test ./hack/production -run 'TestRestoreTrafficCutover' -count=10 -timeout=180s`、
  `go test ./hack/production -run 'Test(RestoreTrafficCutover|RestoreCutoverOperation)' -count=5 -timeout=180s`
  通过。
- A549 收紧 post-restore audit cutover evidence digest schema：
  `audit-restored-instance.sh` 读取 `kubebrain.restore-cutover.state.v1` 时仍只要求
  artifact digest 非空；如果 state 与 `kubebrain.restore-cutover.receipt.v1` 同时携带
  自洽但非法的短 digest、大写 digest 或非 hex digest，旧逻辑可能继续发布
  `kubebrain.post-restore-audit.receipt.v1`。现在 audit 入口对 cutover state、cutover
  receipt 和既有 audit receipt 都要求小写 `[a-f0-9]{64}`；fixture 改用真实 SHA-256
  形状，并覆盖非法 digest 在生成审计 receipt 前被拒绝。回归：
  `go test ./hack/production -run 'TestPostRestoreAudit' -count=10 -timeout=180s` 通过。
- A550 收紧 certificate rotation fingerprint schema：`validate-certificate-rotation.sh`
  通过 `openssl x509 -fingerprint -sha256` 读取旧/新客户端证书指纹后直接写入
  state/receipt，旧逻辑未验证输出是否为真实 SHA-256 形状，测试夹具也使用 `aa11/bb22`
  短值。现在 fingerprint helper 本身要求小写 `[a-f0-9]{64}`，既有
  `kubebrain.certificate-rotation.receipt.v1` 复用校验也要求旧/新证书指纹字段满足同一
  schema；fixture 改用真实 SHA-256 形状，并覆盖 openssl 返回短指纹时 begin gate
  失败且不生成 receipt。回归：
  `go test ./hack/production -run 'TestValidateCertificateRotation' -count=10 -timeout=120s`
  通过。
- A551 收紧 destroy logical backup digest schema：`destroy-instance.sh` 通过
  `logical-status FIELD=sha256` 读取逻辑备份摘要后直接写入
  `kubebrain.destroy.state.v1` 与 `kubebrain.destroy.receipt.v1`，旧逻辑只要求 state
  里的 backup digest 非空，测试夹具也返回 `artifact-sha-256`。现在 prepare 阶段、
  state 复读和既有 destroy receipt 复用都要求小写 `[a-f0-9]{64}`；fixture 改用真实
  SHA-256 形状，并覆盖 logical-status 返回短 digest 时拒绝 prepare。回归：
  `go test ./hack/production -run 'TestDestroyInstance' -count=10 -timeout=120s` 通过。
- A552 收紧 Backup operation object receipt 成功门禁：
  `run-backup-operation.sh` 旧逻辑在 `OBJECT_COMMAND` 返回成功后只要求
  `receipt_output` 文件存在，就把该文件 SHA-256 作为 operation 成功 receipt 提交；
  测试 fake 甚至只写 `{"format":"kubebrain.object-backup.receipt.v1"}`。现在 succeed 前
  重新读取本地 logical artifact status，并要求 `kubebrain.object-backup.receipt.v1`
  精确绑定 instance、backup_id、object store、bucket/key、artifact format/SHA/revision、
  created_at、records、leases、object bytes、retention 和 remote verification；非法
  receipt 会 requeue，不会标记 operation 成功。回归：
  `go test ./hack/production -run 'TestBackupOperation' -count=10 -timeout=180s` 通过。
- A553 收紧 PostRestoreAudit operation receipt 成功门禁：
  `run-post-restore-audit-operation.sh` 旧逻辑同样只检查 `receipt_output` 文件存在，
  测试 fake 写 `{"format":"receipt"}` 也会提交 succeed。现在 succeed 前读取
  cutover state header，并要求 `kubebrain.post-restore-audit.receipt.v1` 精确绑定
  operation_id、instance、cutover_operation_id、service UID、target、artifact SHA、
  snapshot revision、replicas、duration/interval、minimum samples、probe revisions 和
  completed/topology/probe 成功布尔字段；非法 receipt 会 requeue。回归：
  `go test ./hack/production -run 'TestPostRestoreAuditOperation' -count=10 -timeout=120s`
  通过。
- A554 收紧 RestoreCutover operation receipt 成功门禁：
  `run-restore-cutover-operation.sh` 旧逻辑在 complete 后只要求 `receipt_output` 存在，
  测试 fake 只写 restore-cutover format 也会提交 succeed。现在 succeed 前读取 durable
  `kubebrain.restore-cutover.state.v1`，并要求
  `kubebrain.restore-cutover.receipt.v1` 精确绑定 operation、instance、service
  namespace/name/UID、source/target、artifact SHA、snapshot revision、replicas 和
  cutover state SHA-256，且 Pod/Endpoint/public data 验证字段均为 true；非法 receipt
  会终止 operation。回归：
  `go test ./hack/production -run 'TestRestoreCutoverOperation' -count=5 -timeout=240s`
  通过。
- A555 收紧 Destroy operation receipt 成功门禁：
  `run-destroy-operation.sh` 旧逻辑在 complete 后只要求 `receipt_output` 存在，测试 fake
  也只写 `{"format":"receipt"}` 就会提交 succeed。现在 succeed 前读取 durable
  `kubebrain.destroy.state.v1`，确认 state header 绑定 operation、instance、
  KubeBrain namespace/statefulset 和 TiDB namespace/cluster，再要求
  `kubebrain.destroy.receipt.v1` 精确绑定 operation、instance、KubeBrain namespace、
  TiDB namespace/cluster、prepare 阶段冻结的 logical backup SHA/revision、
  `resources_absent=true` 和正整数完成时间；
  非法 receipt 会 requeue，不会标记 operation 成功。回归：
  `go test ./hack/production -run 'TestDestroyOperation' -count=10 -timeout=180s`
  通过。
- A556 收紧 CertificateRotation operation receipt 成功门禁：
  `run-certificate-rotation-operation.sh` 旧逻辑在 complete 后只要求 `receipt_output`
  存在，测试 fake 写 `{"format":"receipt"}` 也会提交 succeed。现在 succeed 前读取
  durable `kubebrain.certificate-rotation.state.v1` 和 overlap marker，确认 operation、
  instance、endpoint 以及旧/新证书 fingerprint schema，再要求
  `kubebrain.certificate-rotation.receipt.v1` 精确绑定 endpoint、replicas、旧/新证书
  SHA-256、`pods_unchanged=true`、`old_certificate_rejected=true` 和正整数完成时间；非法
  receipt 会 requeue，不会标记 operation 成功。回归：
  `go test ./hack/production -run 'TestCertificateRotationOperation' -count=10 -timeout=180s`
  通过。
- A557 收紧 Backup operation succeed 前 receipt 复检：
  A552 已在 upload workflow 内校验 `kubebrain.object-backup.receipt.v1`，但 child 校验
  通过后父进程最终只检查 `receipt_output` 仍存在，再提交其 SHA-256；如果本地 receipt
  在 workflow 校验后、operation succeed 前被替换，旧逻辑可能把错误 digest 写入终态。
  现在 `run-backup-operation.sh` 在提交 succeed 前再次调用同一 strict
  `validate_object_receipt`，重新读取 logical artifact status、对象 receipt schema 和
  字节/retention/identity 绑定；末端复检失败会 requeue。回归用第三次 status 调用篡改
  receipt，证明不会提交 Succeeded：
  `go test ./hack/production -run 'TestBackupOperation' -count=10 -timeout=180s`
  通过。
- A558 收紧 BackupDeletion operation succeed 前 final receipt 复检：
  `run-backup-deletion-operation.sh` 已会在 existing receipt 或 hard-link 冲突路径调用
  `validate_operation_receipt`，但新生成 operation receipt 后直接计算 SHA-256 并提交
  succeed。现在在 `sha256sum` 前再次复用同一 strict validator，确认 final receipt 仍精确
  绑定 source/pre/post/deletion receipts 和 manifest SHA-256；任何本地替换或 schema 漂移
  都会 fail closed，不会提交错误 digest。回归：
  `go test ./hack/production -run 'TestBackupDeletionOperation' -count=5 -timeout=240s`
  通过。
- A559 收紧 RestoreCutover/PostRestoreAudit durable state schema：
  恢复切流 state 文件旧读取路径只消费前几个 tab 字段，未知行、空行、额外列或缺失
  Pod/Service 证据可能被后续 receipt SHA 绑定掩盖。现在
  `switch-restore-traffic.sh`、`audit-restored-instance.sh` 以及两个 operation
  runner 的 succeed 前 validator 都要求 `kubebrain.restore-cutover.state.v1` 精确包含
  1 个 HEADER、1 个 SERVICE、`EXPECTED_REPLICAS` 个 source POD 和 target POD 行，
  且各行字段数封闭；非 canonical state 会 fail closed，不会切流、发布 audit receipt
  或提交 operation succeed。回归：
  `go test ./hack/production -run 'Test(RestoreTrafficCutover|RestoreCutoverOperation|PostRestoreAudit)' -count=1`
  通过。
- A560 收紧 CertificateRotation durable state/marker schema：
  证书轮换 state 旧读取路径只校验 header 字段，operation runner 的 receipt 复检也没有确认
  Pod 快照行数/字段和 overlap marker 的单行封闭格式。现在
  `validate-certificate-rotation.sh` 在 overlap/complete 前要求
  `kubebrain.certificate-rotation.state.v1` 精确包含 header 与
  `EXPECTED_REPLICAS` 个 name/UID/restart/ready Pod 行，并要求 overlap marker 正好一行；
  `run-certificate-rotation-operation.sh` 在提交 succeed 前复用同一封闭 schema，非 canonical
  evidence 会 requeue，不会提交错误 receipt digest。回归：
  `go test ./hack/production -run 'Test(CertificateRotationOperation|ValidateCertificateRotation)' -count=1`
  通过。
- A561 收紧 Destroy durable state/marker schema：
  销毁 state 旧路径只复读 header，RESOURCE/PVC 行和 quiesced/destroyed marker 只在部分
  阶段被隐式消费；未知行、额外列、缺失资源/PVC 证据或伪 marker 可能被 takeover/receipt
  路径绕过。现在 `destroy-instance.sh` 要求 state 精确包含 1 个 HEADER、10 个
  RESOURCE 和 `EXPECTED_PVCS` 个 canonical PVC 行，并要求 quiesced/destroyed marker
  都是绑定 instance/operation 的单行封闭格式；`run-destroy-operation.sh` 在提交 succeed
  前复用同一 schema。非 canonical evidence 会 fail closed/requeue，不会继续删除或提交
  错误 receipt digest。回归：
  `go test ./hack/production -run 'Test(DestroyOperation|DestroyInstance)' -count=1`
  通过。
- A562 收紧 RestoreCutover marker schema：
  恢复切流的 cutover/verified/rollback marker 旧复用路径只读取首行前几列，额外字段、
  额外行或伪造 verified timestamp 可能在幂等重试和 complete 前被忽略。现在
  `switch-restore-traffic.sh` 要求 CUTOVER/ROLLBACK marker 精确绑定目标/源 instance 与
  正整数时间戳，VERIFIED marker 精确包含正整数时间戳，且三类 marker 都必须正好一行；
  verify/complete 在消费已有 marker 前也复用同一封闭 schema。非 canonical marker 会
  fail closed，不会继续数据验证或发布 cutover receipt。回归：
  `go test ./hack/production -run 'TestRestoreTrafficCutover' -count=1` 通过。
- A563 收紧 RestoreCutover operation runner marker 绑定：
  worker takeover 和 succeed 门禁此前只用 `.cutover/.rollback` 文件存在性选择恢复阶段，
  final receipt validator 也只绑定 state/receipt；若本地 evidence 被替换成非 canonical
  marker，runner 层可能与 A562 子脚本门禁不一致。现在
  `run-restore-cutover-operation.sh` 在 rollback marker 触发终态失败前先校验其
  ROLLBACK schema，并在提交 succeed 前要求 CUTOVER 和 VERIFIED marker 都通过同一单行
  封闭 schema。非 canonical marker 会终止为失败，不会提交 operation succeed。回归：
  `go test ./hack/production -run 'TestRestoreCutoverOperation' -count=1` 通过。
- A564 收紧 boundary cleanup durable state schema：
  边界清理 state 旧读取路径只消费 HEADER 和已有 SECRET 行，未知行、额外列或缺失某个
  credential SECRET 行可能被忽略，导致 delete/complete 按残缺 state 漏删凭据并发布
  cleanup receipt。现在 `cleanup-instance-boundaries.sh` 要求
  `kubebrain.boundary-cleanup.state.v1` 精确包含 1 个 HEADER 和按
  `CREDENTIAL_SECRETS` 顺序排列的全部 SECRET name/UID 行；非 canonical state 会
  fail closed，不会删除资源或发布 cleanup receipt。回归：
  `go test ./hack/production -run 'TestBoundaryCleanup' -count=1` 通过。
- A565 收紧 PostRestoreAudit runner 参数非空门禁：
  `run-post-restore-audit-operation.sh` 的参数解析表达式实际只排除 `null`，会让空
  state/receipt 路径、Service 身份、target instance、public endpoint、audit prefix 或
  receipt output 进入子审计脚本；同时 TSV 交给 Bash whitespace IFS 后会让中间空字段左移，
  使错误落到错误参数上。现在 runner 在 jq 输出 TSV 前拒绝所有空必填字符串，并用 `-`
  哨兵保留可选 kube context/path 的字段位置。回归：
  `go test ./hack/production -run 'TestPostRestoreAuditOperation' -count=1` 通过。
- A566 收紧其余 shell operation runner 参数 TSV 边界：
  Backup、BackupDeletion、RestoreCutover、CertificateRotation 和 Destroy runner 同样在
  `jq @tsv` 后用 Bash whitespace IFS 读取参数；空必填字段可能造成字段左移，绕过原本的
  shell 非空检查或把错误归到错误参数。现在这些 runner 都在 jq 输出 TSV 前拒绝空必填
  字符串，optional metrics/kube context/kubeconfig 字段继续用哨兵保留位置。回归：
  `go test ./hack/production -run 'Test(BackupOperation|BackupDeletionOperation|RestoreCutoverOperation|CertificateRotationOperation|DestroyOperation)' -count=1`
  通过。
- A567 补齐 backendShim 直连 `IgnoreValue/IgnoreLease` 契约：
  对照 `/root/etcd/server/etcdserver/txn/put.go` 的 `checkAndGetPrevKV/put`、
  `/root/etcd/server/etcdserver/api/v3rpc/key.go` 的 Put/Txn 校验，以及
  `/root/etcd/tests/integration/clientv3/kv_test.go` 的 ignore option 集成测试，
  上游在 key 缺失时返回标准 `InvalidArgument: etcdserver: key not found`，
  key 存在时从当前 KV 保留 value 或 lease 后再提交 Put。KubeBrain RPCServer 与 staged
  Txn 已在上层展开这些选项，但 `BackendShim.Create/Update/Put` 直连仍返回
  `Unimplemented`，给内部调用和未来 fast path 留下旧兼容缺口。现在 shim 通过统一
  effective Put helper 展开 ignore 选项；`Put` 在同一 mutation key lock 内基于本轮
  CAS 读取到的当前 KV 保留字段，`Create/Update` 也复用该 helper，并在缺失 key 时返回
  标准 key-not-found。回归：
  `go test ./pkg/server/etcd -run 'TestBackendShim(PutExpandsIgnoreOptions|ComparePutsExpandIgnoreOptions|IgnoreOptionsRequireExistingKey)'`
  以及 `go test ./pkg/server/etcd -run 'Test(PutIgnore|TxnComparePutIgnore|TxnIgnoreOptions|BackendShim)'`
  通过。
- A568 修复 linearizable MemberList 的 auth/barrier 顺序：
  对照上游 etcd gRPC auth 在 handler 前完成、`/root/etcd/server/etcdserver/api/v3rpc/member.go`
  只在已放行请求里执行 `MemberList` 的顺序，KubeBrain 旧实现会在
  `MemberList(linearizable=true)` 中先执行 read barrier 再检查 token；未认证请求在无
  leader、barrier 超时或 RBAC 故障时可能返回 barrier 错误，而不是 etcd 兼容的
  `ErrUserEmpty`。现在 `MemberList` 先执行非 root auth，再按 `Linearizable` 触发
  `SyncReadRevision`；认证用户仍能看到原始 barrier status。回归：
  `go test ./pkg/server/etcd -run 'TestMemberListLinearizable' -count=1` 通过。
- A569 收紧 MemberList advertised URL 发布门禁：
  对照 `/root/etcd/server/etcdserver/api/v3rpc/member.go` 原样返回 member
  `PeerURLs/ClientURLs`、`/root/etcd/client/v3/client.go` 的 `Client.Sync` 直接把
  非 learner member 的 `ClientURLs` 作为后续 resolver endpoint，DBaaS release gate
  不能把重复 URL 静默归一化后放行。旧 `validate-instance-ready.sh` 在编码
  `EXPECTED_ADVERTISE_CLIENT_URLS` 和运行时 MemberList 时使用 `sort | unique`，
  会让重复 advertised client URL、重复 runtime client URL 或重复 runtime peer URL
  看起来与期望集合一致。现在发布门禁在归一化前拒绝期望 advertised URL 的空/重复项，
  并要求运行时每个 member 的 peer/client URL 数组本身无重复且与声明精确匹配；重复
  拓扑不会继续进入 endpoint health 阶段。回归：
  `go test ./hack/production -run 'TestValidateInstanceReady' -count=1` 通过。
- A570 支持 etcd repeated-name initial-cluster peer URL 语义：
  对照 `/root/etcd/client/pkg/types/urlsmap.go` 的 `NewURLsMap` 和
  `/root/etcd/client/pkg/types/urlsmap_test.go`，上游允许
  `mem1=urlA,mem1=urlB` 聚合同一 member 的多个 peer URL。KubeBrain 旧
  `ParseInitialCluster` 把重复 member name 直接拒绝，且本机 identity 校验、
  MemberList header 与 Status leader 都按单个 `host:port` CRC32 判定；合法
  multi-peer 配置无法启动或会产生与静态 MemberList 不一致的 member ID。现在
  `ParseInitialCluster` 按 member name 聚合 peer URLs，排序后稳定生成 member
  ID；显式 advertised client URL 保持调用方顺序，派生 client URL 去重排序；
  option 校验逐 peer URL 查找本机 identity，MemberList header 和 Status leader
  也通过静态 member peer URL 反查 ID。`validate-instance-ready.sh` 同步按 etcd
  repeated-name 语义聚合 `EXPECTED_INITIAL_CLUSTER`，再与运行时 MemberList 精确比对。
  回归：
  `go test ./pkg/server/etcd -run 'Test(ParseInitialCluster|StaticMemberID|MemberList|StatusLeader)' -count=1`、
  `go test ./cmd/option -run 'TestInitialCluster' -count=1` 和
  `go test ./hack/production -run 'TestValidateInstanceReady' -count=1` 通过。
- A571 固定 HTTP gateway 代理请求身份标记：
  A24 已禁止带 `grpcgateway-accept` 的 gateway 回环请求借服务端证书 CN 作为终端用户，
  但该 metadata 过去依赖 grpc-gateway 默认从客户端 HTTP `Accept` 头派生；省略
  `Accept` 的 JSON 请求在 TLS-only + client-cert-auth 部署里可能失去代理标记，进而
  回退到本机 gateway mTLS 身份。现在 `newGRPCGatewayMux` 对每个 HTTP request 显式
  注入内部 `grpcgateway-accept=kubebrain-grpc-gateway` 标记，不改变 JSON wire
  contract，也不依赖用户 header；现有 auth 层继续按该标记拒绝证书身份回退。回归：
  `go test ./pkg/endpoint -run 'TestGRPCGatewayUsesGeneratedEtcdJSONContract' -count=1`
  覆盖无 `Accept` 请求仍携带代理标记。
- A572 补齐 operation queue 与 OIDC claim 本地 schema 校验：
  A542 已把 operation type、parameters digest 和成功 receipt digest 从 CRD admission
  下沉到 Go 队列层，但 `Queue.Submit` 仍未镜像 CRD 对 operation ID、instance、
  `maxAttempts` 和参数 Secret 引用的约束；fake-client、旧集群或直接队列调用可能接受
  真实 apiserver 会拒绝的对象。现在 Go 层按 CRD schema 拒绝非法 operation ID/
  instance、超过 100 的 attempts、非法 Secret name/key；OIDC `kubebrain_instances`
  claim 同步只接受 `*` 或 CRD instance 标识符，避免异常 claim 值进入授权集合。回归：
  `go test ./hack/production/internal/operationqueue ./hack/production/internal/operationapi -run 'TestQueueRejectsSpecDriftAndInvalidCompletion|TestOIDCAuthenticator' -count=1`
  通过。
- A573 补齐 backup scheduler 本地 BackupPolicy schema 预检：
  A572 让 operation queue 在 Submit 时兜底拒绝非法 instance/maxAttempts 等字段，但
  backup scheduler 在读取参数模板和创建 immutable 参数 Secret 前仍只检查 policy
  instance 非空、retention 正数和 template 引用非空；fake-client 或旧集群路径可能把
  真实 BackupPolicy CRD 会拒绝的对象推进到模板读取、Secret 创建或 Submit 错误阶段。
  现在 scheduler 在 reconcile policy 起点按 BackupPolicy CRD schema 本地拒绝非法
  tenant、instance、interval/retention 上下限、`maxAttempts>100` 以及 template Secret
  name/key；回归证明无模板 Secret 时坏 policy 仍先被 schema 预检拒绝且不创建
  Operation/Secret。回归：
  `go test ./hack/production/internal/backupscheduler -run 'TestReconcileRejectsPolicySchemaBeforeTemplateRead|TestReconcileIsDeterministicAcrossReplicas' -count=1`
  通过。
- A574 收紧 parameter broker Bearer header 解析：
  parameter broker 通过 Kubernetes TokenReview 校验 executor projected token，但旧
  `bearerToken` 只接受大小写固定的 `"Bearer "` 前缀，同时又对 token 本体执行
  `TrimSpace`；`Bearer  token`、尾随空白或内部空白 token 可能被静默规范化后继续送审，
  而小写 `bearer` 反而被拒绝。现在 broker 与 operation API 一致地大小写不敏感解析
  Bearer scheme，并在 TokenReview 前拒绝空 token、含任何空白的 token 和超过 16 KiB
  的 token。回归：
  `go test ./hack/production/internal/parameterbroker -run 'TestBearerTokenParsingIsStrictAndCaseInsensitive|TestHandlerReturnsOnlyCurrentTypeBoundWorkerParameters' -count=1`
  通过。
- A575 收紧 operation API OIDC Bearer header 解析：
  外部 operation API 的 OIDC 鉴权已使用 `EqualFold` 接受大小写不敏感的 Bearer scheme，
  但旧实现对整个 Authorization header 和 token 本体都执行 `TrimSpace`；
  `Bearer  <valid-jwt>`、尾随空白或前导空白 header 会被规范化成合法 JWT 后继续验签。
  现在 OIDC API 与 parameter broker 一样先拒绝 header 前后空白、空 token 和 token
  内任何空白，再执行 JWT parse/issuer/audience/claim 校验。回归：
  `go test ./hack/production/internal/operationapi -run 'TestOIDCAuthenticator(RejectsMalformedBearerHeader|ValidatesIdentityAndRefreshesUnknownKey|FailsClosed)' -count=1`
  通过。
- A576 收紧 `operationctl --action parameters` projected token 文件解析：
  A574/A575 已让 broker 和外部 API 拒绝畸形 Bearer header，但 `operationctl` 客户端读
  broker projected token 文件时仍用 `TrimSpace`；包含内部换行、前后空格或内部空格的文件
  可能被拼进 Authorization 后才由 HTTP transport/broker 拒绝。现在客户端只容忍文件末尾
  CR/LF，token 本体为空、含任何空白或超过 16 KiB 都在读 CA/发请求前 fail closed。回归：
  `go test ./hack/production/cmd/operationctl -run 'TestBrokerParameters(UsesTLSBearerAndFencingIdentity|RejectsInvalidTokenFile)' -count=1`
  通过。
- A577 收紧 Prometheus bearer token 文件解析：
  metering archive 通过 `--prometheus-bearer-token-file` 读取跨网络 Prometheus token，
  旧实现同样对整文件执行 `TrimSpace` 并只拒绝 CR/LF；前后空格会被静默吞掉，内部空格或
  tab 会被拼进 Authorization 后才由 HTTP transport 或 Prometheus 拒绝。现在只容忍文件
  末尾 CR/LF，token 本体为空、含任何空白或超过 16 KiB 都在创建 collector 和发起 HTTP
  请求前 fail closed。回归：
  `go test ./hack/production/cmd/metering-archive -run TestReadToken -count=1`
  通过。
- A578 收紧 metering archive 生产清单的 Prometheus 凭据基线：
  A517/A577 已让二进制支持 CA/token 文件边界，但 `kubebrain-metering-archive` CronJob
  仍默认使用集群内明文 `http://prometheus-operated...`，且未挂载 bearer token/CA；直接套用
  production 清单会绕过文档要求的 HTTPS、server name 和 token 鉴权。现在清单改用
  `https://prometheus-operated.kubebrain-system.svc.cluster.local:9090`，挂载
  `kubebrain-metering-archive-prometheus` Secret 的 `ca.crt`/`token`，并显式传入
  `--prometheus-ca-file`、`--prometheus-bearer-token-file` 和
  `--prometheus-server-name`；manifest 回归禁止 `--prometheus-url=http://` 回退，并固定
  Secret 只读 mount/fsGroup。回归：
  `go test ./deploy/production -run TestMeteringArchiveCronJobIsFailClosedAndImmutable -count=1`
  通过。
- A579 固定 operation API/parameter broker TLS Secret mount 契约：
  TLS 热加载已按 1 MiB 文件上限和 key pair 校验 fail closed，但生产清单的 API/broker
  TLS Secret volume 仍未固定 `items/defaultMode/fsGroup`；operation API 还只依赖镜像
  `USER 65532:65532`，没有在 Pod securityContext 中显式声明 UID/GID。现在 API 和 broker
  都以 `runAsUser/runAsGroup/fsGroup=65532` 运行，TLS Secret 只挂载 `tls.crt`/`tls.key`
  两个 key，mode 固定为 `0440`，并用共享 manifest helper 校验只读 volumeMount、Secret
  名和键集合，防止误挂载额外 Secret 内容或镜像 USER 漂移造成凭据读取差异。回归：
  `go test ./deploy/production -run 'Test(OperationAPIIsFailClosedAndHardened|OperationParameterBrokerOwnsTheOnlyExecutorParameterSecretPermission)' -count=1`
  通过。
- A580 固定 certificate rotation hook Secret allowlist：
  证书轮换 executor 通过 `kubebrain-certificate-rotation-executor-hooks` Secret 注入
  `publish-overlap` 和 `publish-final` 两个受审计 hook，但旧 volume 没有 `items`
  allowlist，Secret 中任何额外 key 都会以同样可执行权限出现在 hook 目录；`defaultMode: 365`
  也让八进制意图不直观。现在清单显式写 `defaultMode: 0555`，并只挂载
  `publish-overlap`/`publish-final` 两个 key；manifest 回归确保只有 certificate rotation
  executor 有 hooks volume，其他五类 executor 不会挂载该 Secret。回归：
  `go test ./deploy/production -run TestOperationExecutorsAreTypeIsolatedFailClosedTemplates -count=1`
  通过。
- A581 固定数据面 client/peer TLS Secret allowlist：
  A579 收紧了 operation API/broker 的 TLS Secret mount，但核心 `kubebrain-tls.yaml`
  仍直接挂载整个 `kubebrain-client-tls` 和 `kubebrain-peer-tls` Secret，未固定
  `defaultMode` 或 `ca.crt/tls.crt/tls.key` item 集合。现在 client/peer TLS volume 都以
  `0440` 只读 mode 只挂载这三个 key，manifest 测试在 TLS 清单分支同时固定证书参数、
  client/peer Secret 名、mount path 和 key/path allowlist，防止额外 Secret 内容进入数据面
  Pod。回归：
  `go test ./deploy/production -run TestProductionManifestsProvideStableCompleteMembership -count=1`
  通过。
- A582 固定 operation executor projected broker token 权限：
  六类 executor 访问 parameter broker 的 projected ServiceAccount token 已使用专用 audience
  和 3600 秒过期时间，但旧 projected volume 未固定 `defaultMode`，会依赖 Kubernetes 默认
  文件权限。现在所有 executor 模板都把 `parameter-token` projected volume mode 固定为
  `0440`，manifest 回归同时校验 token path、audience、expirationSeconds 和 mode，确保
  executor 只使用专用 broker token，且不回退到默认 API token 或更宽文件权限。回归：
  `go test ./deploy/production -run TestOperationExecutorsAreTypeIsolatedFailClosedTemplates -count=1`
  通过。
- A583 对齐 etcd 重复 auth metadata 首值语义：
  对照 `/root/etcd/server/auth/store.go` 的 `AuthInfoFromCtx`，上游从
  `token`/`authorization` metadata 中取第一个值并只对该值去除精确 `Bearer ` 前缀；
  KubeBrain 旧实现遇到同名 metadata 多值会直接返回 `ErrInvalidAuthToken`，让代理重复
  注入 header 或客户端重复设置 per-RPC credential 时偏离 etcd。现在
  `authCallerFromContext` 与上游一致使用首个 credential、保留原始首值转发给 leader，
  且首值为空仍 fail closed。回归：
  `go test ./pkg/server/etcd -run TestAuthCallerUsesFirstRepeatedMetadataToken -count=1`
  通过。
- A584 对齐 client-cert auth 管理 RPC 错误码：
  对照 `/root/etcd/server/auth/store.go` 的 `IsAdminPermitted` 与
  `/root/etcd/server/etcdserver/apply/auth.go` 的 `UserGet`/`RoleGet` 分支，上游在
  client certificate CommonName 为空时返回 `ErrUserEmpty`，CommonName 非空但 auth store
  中没有对应用户时返回 `ErrUserNotFound`；KubeBrain 旧的 `isRoot` 布尔检查会把这些
  admin/self/role guard 统一折叠成 `ErrPermissionDenied`。现在 auth 管理 RPC 使用
  `adminError` 保留 etcd 的空用户、旧修订、用户不存在和非 root permission denied
  区分；KV/Lease 普通权限检查仍维持 etcd 的不存在用户 permission denied 语义。回归：
  `go test ./pkg/server/etcd -run TestAuthRPCClientCertificateAdminErrorsMatchEtcd -count=1`
  通过。
- A585 对齐 `UserChangePassword` 空密码语义：
  对照 `/root/etcd/server/etcdserver/v3_server.go` 与
  `/root/etcd/server/auth/store.go`，上游 `UserAdd` 会对空明文密码生成 bcrypt hash，
  但 `UserChangePassword` 只有在新密码非空时才 hash；空明文且空 `HashedPassword`
  会作为空哈希落库，随后认证返回 `ErrAuthFailed`。KubeBrain 旧的
  `authChangedPassword` 会把空字符串 hash 成可登录密码，导致 clientv3 空密码变更后
  行为偏离 etcd。现在密码变更路径只 hash 非空明文，否则直接 base64 decode
  `HashedPassword`，空哈希保持不可认证；常规非空密码变更仍生成 bcrypt。回归：
  `go test ./pkg/server/etcd -run TestAuthManagerEmptyPasswordChangeMatchesEtcd -count=1`
  通过。
- A586 对齐 `Authenticate` 并发 auth revision 复查：
  对照 `/root/etcd/server/etcdserver/v3_server.go` 的 `Authenticate`，上游在
  `CheckPassword` 和 token 写入之间发现 auth revision 变化会重试，避免并发密码变更后
  仍签出旧凭据。KubeBrain 旧的 `authTokenManager.authenticate` 只基于一次
  auth snapshot 校验密码并签 token；若校验后发生密码变更或角色变更，可能使用旧
  revision/generation 签发。现在密码校验成功后重新读取 snapshot：revision/enable
  状态变化时重试；并发密码变更会让旧密码返回 `ErrAuthFailed`，无关 auth mutation
  会签发包含最新 auth revision 的 token。回归：
  `go test ./pkg/server/etcd -run TestAuthTokenAuthenticateRetriesAfterAuthRevisionChange -count=1`
  通过。
- A587 对齐 root-only maintenance/member RPC 的 client-cert auth 错误码：
  对照 `/root/etcd/server/etcdserver/api/v3rpc/maintenance.go`、
  `/root/etcd/server/etcdserver/server.go` 与
  `/root/etcd/server/auth/store.go`，上游 Status/Alarm GET 等读类只要求存在
  auth info，但 Defragment/Hash/HashKV/Snapshot/MoveLeader/Downgrade 以及 member
  mutation 会调用 `IsAdminPermitted`，因此空 CommonName 返回 `ErrUserEmpty`，
  CommonName 非空但用户不存在返回 `ErrUserNotFound`，非 root 用户返回
  `ErrPermissionDenied`。KubeBrain 旧的 `requireAuthenticated(root=true)` 只用
  `isRoot` 布尔值，会把前两类证书身份折叠成 permission denied。现在 root-required
  分支复用 `adminError`，同时 auth disabled 和读类 RPC 行为保持不变。回归：
  `go test ./pkg/server/etcd -run 'Test(Maintenance|Member)RootAuthorizationClientCertificateErrorsMatchEtcd' -count=1`
  通过。
- A588 对齐 JWT auth-token option 分解：
  对照 `/root/etcd/server/auth/store.go` 的 `decomposeOpts`，上游使用
  `strings.Split(option, "=")`，因此 option value 内多余 `=` 会被判为
  `ErrInvalidAuthOpts`；空 option key 不是分解错误，而是进入 JWT provider 后作为未知
  option warning-only 忽略。KubeBrain 旧 parser 用 `SplitN` 并拒绝空 key，导致
  `priv-key=/path/with=equals` 被错误接受、`=ignored` 被错误拒绝。现在分解规则与
  上游一致：多等号 option fail closed，空 key 由未知 option 路径忽略；JWT key 文件
  1 MiB 上限仍作为生产 fail-closed 扩展保留。回归：
  `go test ./pkg/server/etcd -run TestJWTProviderOptionSyntaxMatchesEtcd -count=1`
  通过。
- A589 对齐空 `--auth-token` 的 nop provider 语义：
  对照 `/root/etcd/server/auth/store.go` 与 `/root/etcd/server/auth/nop.go`，上游
  `--auth-token=""` 会分解为 token type 空字符串并创建 nop provider；AuthEnable 后
  即使用户名密码正确，token `assign` 仍返回 `ErrAuthFailed`，且任何 token 验证都失败。
  KubeBrain 旧的 `parseAuthTokenProvider("")` 把空字符串等同 simple，显式空配置会
  意外签发可用 simple token。现在 `SetAuthConfiguration("")` 启用 token manager 的
  nop 模式：仍执行 auth enabled、用户存在、NoPassword 和 bcrypt 检查，但正确密码也
  返回 `ErrAuthFailed`，verify 固定返回 `ErrInvalidAuthToken`；默认配置仍是显式
  `simple`，不受影响。回归：
  `go test ./pkg/server/etcd -run TestAuthTokenEmptyProviderMatchesEtcdNop -count=1`
  通过。
- A590 对齐 simple auth-token option 分解：
  对照 `/root/etcd/server/auth/store.go` 的 `decomposeOpts` 与 `NewTokenProvider`，
  上游会先校验所有 `key=value` option 的形状和重复 key，再按 token type 分派；
  `simple,foo=bar` 或 `simple,=ignored` 因此会被接受并忽略，而
  `simple,foo`、`simple,foo=bar=baz`、重复 key 仍是无效配置。KubeBrain 旧实现只把
  精确 `simple` 当 simple provider，导致合法的上游兼容配置在 endpoint validate 或
  启动时失败。现在 auth-token parser 先执行通用分解，再让 `simple` 和空 provider
  使用各自分支，JWT 继续执行 key/TTL 校验。回归：
  `go test ./pkg/server/etcd -run TestAuthTokenProviderSimpleOptionsMatchEtcd -count=1`
  通过。
- A591 对齐 JWT `ttl` 非正 duration 语义：
  对照 `/root/etcd/server/auth/options.go` 的 `jwtOptions.ParseWithDefaults`/`Parse`，
  上游只用 `time.ParseDuration` 校验 `ttl`，因此 `ttl=0s` 和 `ttl=-1s` 都是可启动
  配置；过期时间会随签发逻辑自然落在当前或过去，而不是在 provider 初始化阶段失败。
  KubeBrain 旧实现额外拒绝 `parsed <= 0`，导致上游可接受的 JWT provider 配置被
  endpoint validate 或启动拦截。现在只拒绝无法解析的 duration，保留默认 TTL 和 key
  校验逻辑。回归：
  `go test ./pkg/server/etcd -run TestJWTProviderNonPositiveTTLMatchesEtcd -count=1`
  通过。
- A592 对齐公共 `UserAdd` 的 `HashedPassword` 忽略语义：
  对照 `/root/etcd/server/etcdserver/v3_server.go` 的 `UserAdd` 与
  `/root/etcd/server/auth/store.go` 的 `selectPassword`，上游公共 RPC 在非
  `NoPassword` 用户添加时总是基于 `Password` 重新生成 bcrypt hash，并覆盖/清空
  `HashedPassword`；因此仅传畸形 `HashedPassword` 且明文为空会创建空密码用户，
  不会返回 decode 错误。KubeBrain 旧 `authPassword` 会在 `Password==""` 时尝试
  解码 `HashedPassword`，把上游可接受的 public request 错误拒绝。现在 `UserAdd`
  一律 hash public `Password`，`UserChangePassword` 仍保持上游“明文为空才使用
  `HashedPassword`”语义。回归：
  `go test ./pkg/server/etcd -run 'TestAuthManager(UserAddIgnoresHashedPasswordLikePublicEtcd|BootstrapErrors)' -count=1`
  通过。
- A593 对齐 JWT claim 解析与授权错误分层：
  对照 `/root/etcd/server/auth/jwt.go` 的 `tokenJWT.info`、
  `/root/etcd/server/auth/store.go` 的 `isOpPermitted` 与
  `/root/etcd/server/etcdserver/v3_server.go` 的 `doSerialize`，上游 JWT provider
  只要求 `username` claim 是 string、`revision` claim 存在且可作为数字读取；
  空用户名或 `revision=0` 不在 token parse 阶段返回 `ErrInvalidAuthToken`，而是由
  后续 KV/admin 授权分别落到 `ErrPermissionDenied`、`ErrUserEmpty` 或 root admin
  允许。KubeBrain 旧 verify 直接拒绝空用户名和 0 revision，且 `AuthInfoFromCtx`
  过早拒绝旧 JWT。现在 JWT verify 只校验 claim 存在，key 权限检查按 etcd 顺序处理
  revision 0、旧 revision 和权限；admin/root 语义继续忽略 auth revision。回归：
  `go test ./pkg/server/etcd -run 'TestJWT(EmptyAndZeroClaimsMatchEtcdAuthorization|ManagerUsesAuthRevisionAndRejectsOldToken)' -count=1`
  通过。
- A594 对齐 `RoleGrantPermission` 同 key 搜索语义：
  对照 `/root/etcd/server/auth/store.go` 的 `RoleGrantPermission` 与 `permSlice.Less`，
  上游只按 permission key 排序，并用 `sort.Search` 找第一个 `Key >= request.Key`
  的条目；当同一 key 已存在多个不同 `RangeEnd` 时，重复 grant 其中非首个 range
  不会全表精确查找，而会继续追加一个同 range permission。KubeBrain 旧实现遍历全表
  精确替换，导致 `RoleGet` 在该边界下少返回重复 permission。现在 grant/update
  逻辑按上游的单点 search 执行；revoke 仍与上游一样删除所有 exact key/range 匹配项。
  回归：
  `go test ./pkg/server/etcd -run TestAuthManagerRoleGrantPermissionSameKeySearchMatchesEtcd -count=1`
  通过。
- A595 对齐 JWT `revision` claim 的 `MapClaims` 数字转换：
  对照 `/root/etcd/server/auth/jwt.go` 的 `tokenJWT.info`，上游用
  `jwt.MapClaims` 读取 token claim，并要求 `revision` 的动态类型为 `float64`；
  因此 JSON 小数 revision 会被接受并在 `uint64(revision)` 时截断，超过 float64
  精确整数范围的 revision 也按 Go 的 float64 转换结果处理。KubeBrain 旧 verify
  用结构化 `uint64` 字段解码，错误拒绝 `revision: 1.5`，并保留了上游不会保留的
  大整数精度。现在 JWT verify 改用 `MapClaims`，保留签名方法、过期时间和当前时间
  覆盖校验。回归：
  `go test ./pkg/server/etcd -run TestJWTProviderRevisionFloatCoercionMatchesEtcd -count=1`
  通过。
- A596 对齐 Lease read 的 future JWT revision 后置检查：
  对照 `/root/etcd/server/etcdserver/v3_server.go` 的 `checkLeaseTimeToLive` 与
  `checkLeaseLeases`，上游 lease TTL(Keys=true)/Leases 的 key 授权先由
  `IsRangePermitted` 拒绝旧 revision；授权完成后的复查比较的是 auth store 是否在请求
  期间变化，而不是 token revision 是否等于当前 revision。因此携带 future auth revision
  的合法 JWT 会通过 lease key 读取。KubeBrain 旧 `authorizeLeaseKeys` 复用
  `ensureAuthRevision`，把 future JWT 误判为 `ErrAuthOldRevision`。现在 lease key
  授权后只比较进入请求时的 auth snapshot revision 与最新 revision；旧 token、权限不足
  和并发 auth mutation 仍按 etcd 错误返回。回归：
  `go test ./pkg/server/etcd -run TestAuthLeaseFutureJWTRevisionMatchesEtcd -count=1`
  通过。
- A597 对齐 `UserGet` 空用户名缺失身份错误码：
  对照 `/root/etcd/server/etcdserver/v3_server.go` 的 `AuthInfoFromCtx` 与
  `/root/etcd/server/etcdserver/apply/auth.go` 的 `UserGet`，上游 auth 开启且请求没有
  token/client-cert 时会把 auth info 留空；`UserGet(Name="")` 命中 self-exception
  分支，随后由 auth store 查询空用户名并返回 `ErrUserNotFound`，而不是在 RPC
  入口提前返回 `ErrUserEmpty`。KubeBrain 旧 `authCallerFromContext` 在缺失身份时
  直接报 `ErrUserEmpty`，导致该空用户名边界错误码不一致。现在仅对
  `UserGet(Name="")` 保留上游 fallthrough；普通未认证 `UserGet("alice")` 仍返回
  `ErrUserEmpty`。回归：
  `go test ./pkg/server/etcd -run TestAuthRPCUserGetEmptyNameWithoutIdentityMatchesEtcd -count=1`
  通过。
- A598 对齐 Range/RangeStream 的 read barrier 与 auth 顺序：
  对照 `/root/etcd/server/etcdserver/v3_server.go` 的 `Range`、`RangeStream` 与
  `doSerialize`，上游非 serializable 读会先执行 `LinearizableReadNotify`，随后才通过
  `doSerialize` 解析 auth info 并检查 key 权限；因此 auth 开启但请求缺失身份时，如果
  leader read barrier 先失败，客户端会看到可重试的 barrier status，而不是
  `ErrUserEmpty`。KubeBrain 旧 unary `Range` 和真正的 recursive `RangeStream` 在入口
  先调用 `authCallerFromContext`，会把 leader/barrier 故障遮蔽成 auth 错误。现在普通
  Range 和非 fallback RangeStream 在需要线性读栅栏时先同步 revision，再执行 auth；
  explicit serializable 读仍不触发 barrier，继续返回 `ErrUserEmpty`。历史 follower
  proxy 分支因需要透传 token 保持独立路径。回归：
  `go test ./pkg/server/etcd -run TestAuthRangeReadBarrierPrecedesAuthLikeEtcd -count=1`
  通过。
- A599 对齐 `Authenticate` 的前置 linearizable read barrier：
  对照 `/root/etcd/server/etcdserver/v3_server.go` 的 `Authenticate`，上游在
  `CheckPassword` 和 token assign/retry 之前先执行 `LinearizableReadNotify`；因此
  leader/read-barrier 故障会优先于错误密码、未知用户或 auth 未启用返回。A295 已保证
  成功 Auth 响应 header 在返回前同步到 leader revision，但 KubeBrain 旧
  `Authenticate` 仍先校验密码并签 token，最后才通过 `authRPCHeader` 做 barrier；
  barrier 故障会被 `ErrAuthFailed`/`ErrAuthNotEnabled` 遮蔽。现在 `Authenticate`
  入口先执行 `SyncReadRevision` 并保留成功响应的 header barrier；失败关闭使用标准
  可重试 gRPC status。回归：
  `go test ./pkg/server/etcd -run TestAuthenticateReadBarrierPrecedesPasswordCheck -count=1`
  通过。
- A600 对齐 `AuthStatus` 的 optional auth token 解析：
  对照 `/root/etcd/server/etcdserver/v3_server.go` 的 `processInternalRaftRequestOnce`
  和 `/root/etcd/server/auth/store.go` 的 `AuthInfoFromCtx`，上游 `AuthStatus` 不要求
  登录身份；auth 开启且请求没有 token/client-cert 时仍允许返回状态。但如果请求显式
  携带了 token metadata，入口会解析 token，坏 token 返回 `ErrInvalidAuthToken`，
  不会被 `AuthStatus` 静默忽略。KubeBrain 旧 `AuthStatus` 只读取 auth snapshot 并填
  header，导致坏 token 下仍成功。现在 token metadata 提取逻辑与普通 auth caller
  共用：无 token 允许，空/坏 token 拒绝，合法 token 允许。回归：
  `go test ./pkg/server/etcd -run TestAuthStatusRejectsInvalidTokenWhenEnabled -count=1`
  通过。
- A601 固化 KV 写路径 future JWT revision 语义：
  对照 `/root/etcd/server/auth/store.go` 的 `isOpPermitted` 与
  `/root/etcd/server/etcdserver/v3_server.go` 的 serialized-read 复查，上游 key
  写授权只拒绝小于当前 auth revision 的旧 token；携带 future auth revision 的合法
  JWT 可以通过 `Put`、`DeleteRange` 和写 `Txn`。但 serialized `Range` 在读取完成后
  会比较 token revision 与当前 auth store revision，不相等则返回 `ErrAuthOldRevision`。
  KubeBrain 当前写路径通过 `caller.require` 和 request-local `withAuthWriteGuard`
  已保持该行为；本次新增 KV 回归测试同时覆盖 future JWT 的三类写成功和读拒绝，防止
  后续把写路径误改成 exact revision fence。回归：
  `go test ./pkg/server/etcd -run TestAuthKVFutureJWTRevisionAllowsWritesButNotSerializedReads -count=1`
  通过。
- A602 收紧高风险 Operation 审计 finalizer release 身份：
  `kubebrain-operation-audit` ValidatingAdmissionPolicy 现在显式只匹配 Namespaced
  `kubebrainoperations`，并在移除 `dbaas.kubebrain.io/operation-audit` finalizer
  的分支要求请求者为
  `system:serviceaccount:kubebrain-operations:kubebrain-operation-archiver`。修复前，
  任意持有主资源 `update` 的身份（例如专用 approver 或被 namespace 绑定的维护身份）
  在对象终态后只要提交形状合法的 archive annotations，就可能绕过
  `operationauditrelease` 的本地 artifact/receipt exact-version 复核而释放审计
  finalizer。生产清单同时补齐 policy/binding 标签和 resource scope；`deploy/production`
  新增测试固定 submitter/approver 的精确 RBAC、Fail/Deny admission、approver-only
  approval 与 archiver-only release，`hack/production` 的 manifest 测试同步固定
  Namespaced scope 与 archiver 身份。回归：
  `go test ./deploy/production -run TestOperationSubmitterApproverAndAuditAdmissionFenceHighRiskChanges -count=1`
  和
  `go test ./hack/production -run TestOperationAuditAdmissionRequiresFinalizerAndReleaseEvidence -count=1`
  通过。
- A603 收紧 Operation approval admission 的类型与成对证据：
  A602 继续审计同一 `ValidatingAdmissionPolicy` 时发现，旧 approval 表达式只以
  `approved-by` 作为触发条件：只写 `approval-id` 的 update 会通过 admission，之后
  `Queue.Approve` 会因已有半截证据拒绝正常审批；专用 approver 也可以给 `Backup` 或
  `PostRestoreAudit` 这类低风险 operation 写入完整 approval annotations，worker 会继续
  执行，但终态 `operationaudit.Artifact.Validate` 又会拒绝低风险类型携带 approval，
  导致审计归档/finalizer release fail closed 卡住。现在 admission 只允许两枚
  annotations 成对出现；首次写入必须同时满足专用 approver 身份、DNS-compatible
  approval ID、Pending phase，以及高风险类型白名单
  `BackupDeletion/RestoreCutover/CertificateRotation/Destroy`。既有 approval 证据仍只能
  完全原样保留。回归：
  `go test ./deploy/production -run TestOperationSubmitterApproverAndAuditAdmissionFenceHighRiskChanges -count=1`、
  `go test ./hack/production -run TestOperationAuditAdmissionRequiresFinalizerAndReleaseEvidence -count=1`
  和
  `kubectl apply --dry-run=client -f deploy/production/kubebrain-operation-audit-admission.yaml`
  通过。
- A604 收紧实例 release gate 的镜像不可变性：
  `hack/production/validate-instance-ready.sh` 原先只要求 `EXPECTED_IMAGE` 非空，并在
  StatefulSet/Pod 上做精确字符串相等；如果发布系统错误传入 `kubebrain:dev` 或任意可变
  tag，且集群也运行该 tag，release gate 会通过，违背生产文档中“必须使用
  `registry/...@sha256:<digest>`”的约束。现在入口直接拒绝缺少 `@sha256:` 的镜像、
  非 64 位小写 hex digest 和带空白的引用；后续仍保留 StatefulSet/Pod 精确字符串比较，
  因而同时证明 digest 形式与实际 rollout 收敛。回归：
  `go test ./hack/production -run TestValidateInstanceReady -count=1` 通过。
- A605 收紧 NetworkPolicy release gate 的探针镜像引用：
  A430 已要求 `PROBE_IMAGE` 使用 digest，但 `validate-network-policy.sh` 的旧正则只锚定
  `@sha256:<64hex>` 后缀，仍会接受包含空白前缀的畸形引用。现在与 A604 的实例 release
  gate 统一为完整引用校验：无空白、单个 `@sha256:` 分隔、64 位小写 hex digest。回归
  扩展覆盖 mutable tag、空白 digest 引用和大写 digest，且均在调用 kubectl 前 fail
  closed：
  `go test ./hack/production -run TestValidateNetworkPolicy -count=1` 通过。
- A606 补齐 operation queue status schema 本地门禁：
  A572 已把 KubeBrainOperation spec 的 CRD 约束下沉到 Go queue 层，但 worker status
  写入仍依赖 apiserver/CRD 拒绝过长 `status.owner`、过长 `status.message`，且失败
  operation 可携带非空但非 SHA-256 的 `receiptSHA256`。fake-client、旧 CRD 或直接队列
  调用可能写入真实生产 CRD 会拒绝的终态/重试状态。现在 `Claim`/`Heartbeat`/`Finish`
  在写入前拒绝超过 253 字符的 owner，`Requeue`/`Finish` 拒绝超过 4096 字符的 message，
  `Finish` 对失败路径的非空 receipt 也要求小写 SHA-256 hex；成功路径仍强制 receipt
  非空。回归：
  `go test ./hack/production/internal/operationqueue -run 'TestQueueRejectsSpecDriftAndInvalidCompletion|TestQueueLifecycleAndExpiredLeaseFencing|TestQueueRequeueConsumesAttemptAndFiltersType' -count=1`
  和 `go test ./hack/production/internal/operationqueue -count=1` 通过。
- A607 固化 operation API 的本地 schema 错误分类：
  A572/A606 让 `Queue.Submit` 在本地拒绝 CRD schema drift，但 operation API 仍用少量
  错误字符串判断 400/500；例如外部请求传入 unsupported operation type 时，真实 queue
  返回普通错误，handler 会把客户端 schema 错误误报为 500。现在 queue 暴露
  `ErrInvalidSpec` sentinel，所有 Submit 前置 schema 拒绝都包装该错误；operation API
  通过 `errors.Is` 统一返回 `400 operation request is invalid`，依赖故障和冲突语义
  不变。回归：
  `go test ./hack/production/internal/operationqueue ./hack/production/internal/operationapi -run 'TestQueueRejectsSpecDriftAndInvalidCompletion|TestHandlerReturnsBadRequestForQueueSchemaValidation' -count=1`
  通过。
- A608 固化 backup scheduler 参数 Secret owner fence：
  backup scheduler 生成的参数 Secret 已是 immutable 并做内容 digest 复核，但
  `ensureParametersSecret` 在遇到同名已存在 Secret 时只检查内容，不检查 controller
  ownerReference。旧 BackupPolicy 删除后重建、旧 CRD/fake-client 路径或手工预置同名
  Secret 时，调度器可能引用不属于当前 policy UID 的 immutable Secret，破坏参数制品
  的 provenance 和 GC 边界。现在已存在参数 Secret 必须只有一个 controller owner，且
  APIVersion/Kind/Name/UID 精确指向当前 KubeBrainBackupPolicy；owner 不匹配时 fail
  closed，且不创建 Operation。回归：
  `go test ./hack/production/internal/backupscheduler -run 'TestReconcileRejectsPreexistingParameterSecretWithWrongOwner|TestReconcileIsDeterministicAcrossReplicas|TestReconcileRejectsPolicySchemaBeforeTemplateRead' -count=1`
  通过。
- A609 对齐 failed operation 的审计 receipt 契约：
  A606 仅按 KubeBrainOperation CRD 的 `receiptSHA256` 字段格式收紧 queue 终态写入，
  但 `operationaudit.Artifact.Validate` 和 object archive receipt 明确要求 Failed
  operation 不携带 execution receipt。旧 `Queue.Finish(succeeded=false, receipt!=empty)`
  可写出 CRD 字段格式合法、但后续审计 artifact 校验必然失败的终态，导致 operation
  finalizer 无法释放。现在失败终态一律拒绝非空 receipt；成功终态仍要求小写 SHA-256
  receipt。回归：
  `go test ./hack/production/internal/operationqueue ./hack/production/internal/operationauditbuilder ./hack/production/operationaudit -run 'TestQueueRejectsSpecDriftAndInvalidCompletion|TestAudit|TestFromOperation|TestArtifact' -count=1`
  通过。
- A610 补齐 operation audit artifact 的 status 长度门禁：
  A606 已让 queue 层拒绝超过 CRD 上限的 `status.owner` 与 `status.message`，但
  `operationaudit.Artifact.Validate` 仍只检查 owner 非空和基础终态字段。旧 CRD、
  fake-client 或直接对象若携带超长 owner/message，archiver 会生成生产 CRD 不会接受的
  audit artifact，扩大不可变对象大小并污染审计链。现在 artifact validator 同步拒绝
  owner 超过 253 字符、message 超过 4096 字符；builder 因复用 validator 自动 fail
  closed。回归：
  `go test ./hack/production/operationaudit ./hack/production/internal/operationauditbuilder -run 'TestOperationAuditRejectsNonterminalAndInvalidReceipt|TestFromOperationBindsImmutableSpecAndTerminalStatus' -count=1`
  通过。
- A611 收紧 operation audit archive object prefix：
  operation archiver 归档 key 由 `object-prefix/namespace/uid.json` 组成，但旧配置只把
  prefix 去掉首尾 `/` 后检查非空；`../audit`、`audit/../other`、重复分隔符或空白前缀
  会直接进入 Object Lock executor，并被后续 release receipt 视为“请求一致”。现在
  `NewArchiveProcessor` 要求 prefix 是规范相对 key prefix：无空白、无 `.`/`..` 段、无
  重复分隔，仍允许现有 `/audit/` 形式标准化为 `audit`。回归：
  `go test ./hack/production/internal/operationarchiver -run 'TestArchiveProcessor(UsesStableIdentityAndReleasesFinalizer|RejectsUnsafeObjectPrefix)' -count=1`
  通过。
- A612 收紧 operation audit archive receipt object key：
  A611 约束了 archiver 请求端生成的 key，但 `operationaudit.ArchiveReceipt.Validate`
  仍只要求 receipt 的 `object_key` 非空。旧工具、手工 release 或被替换的 executor
  只要让 release 端 expected key 与 receipt 一致，就可能把绝对路径、路径穿越、重复
  分隔符或空白 object key 写入不可变审计注解。现在 archive receipt 本身也拒绝非规范
  相对 object key；operationauditrelease 和 operationarchiver 因复用
  `InspectArchiveReceipt` 自动 fail closed。回归：
  `go test ./hack/production/operationaudit ./hack/production/internal/operationauditrelease ./hack/production/internal/operationarchiver -run 'TestArchiveReceiptRejectsUnsafeObjectKey|TestArchiveReceiptRejectsUppercaseDigest|TestReleaseVerifiesArchiveAndRemovesOnlyAuditFinalizer|TestArchiveProcessorUsesStableIdentityAndReleasesFinalizer' -count=1`
  通过。
- A613 收紧 operation audit archive receipt 作用域字段：
  A612 已让 `object_key` 拒绝非规范相对路径，但同一 receipt 的
  `object_store_id`、`bucket`、`version_id` 仍只校验非空。旧工具、手工 release
  或被替换的 executor 可能把带空白/控制字符的对象存储作用域写入不可变审计注解，
  造成肉眼审计、离线比对或注解消费端看到的 scope 与真实对象定位不一致。现在这些
  scope 字段必须是有效 UTF-8、非空且不含任意空白/控制字符；`object_key` 也复用
  同一基础字符门禁后再做相对 key 规范化校验。回归：
  `go test ./hack/production/operationaudit -run 'TestArchiveReceiptRejectsUnsafe(ObjectKey|ScopeFields)|TestArchiveReceiptRejectsUppercaseDigest' -count=1`
  通过。
- A614 收紧 operation audit archiver 配置端对象作用域：
  A613 让 archive receipt 端 fail closed，但 `NewArchiveProcessor` 仍只要求
  `objectStoreID`、`bucket` 非空，且 prefix 的空白检查未覆盖 Unicode 空白/控制字符。
  错误配置会先进入外部 Object Lock executor 的环境变量，再依赖 receipt 校验阻断
  finalizer release，扩大排障面。现在 archiver 入口要求 object store ID、bucket
  和 object prefix 都是有效 UTF-8、非空且不含任意空白/控制字符；prefix 仍额外要求
  规范相对 key 前缀。回归：
  `go test ./hack/production/internal/operationarchiver -run 'TestArchiveProcessorRejectsUnsafe(ObjectPrefix|ObjectScope)|TestArchiveProcessorUsesStableIdentityAndReleasesFinalizer' -count=1`
  通过。
- A615 收紧 operation audit archiver 的归档对象身份来源：
  archiver 的 object key 由 `prefix/namespace/uid.json` 拼出，但
  `operationaudit.Artifact.Validate` 只要求 namespace/name/UID 非空。生产 apiserver
  会约束这些元数据，旧对象、fake-client 路径或手工构造的 operation 仍可能携带 `/`、
  `.`、`..`、空白或控制字符，导致 `path.Join` 清理后生成非预期归档 key，或在 release
  阶段用异常 name 访问 operation。现在 `ArchiveProcessor.Process` 在调用外部 executor
  前要求 namespace、name、UID 都是单段 key-safe 元数据；不符合时 fail fast，且不会
  生成对象归档请求。回归：
  `go test ./hack/production/internal/operationarchiver -run 'TestArchiveProcessorRejectsUnsafeOperationIdentityBeforeExecutor|TestArchiveProcessorUsesStableIdentityAndReleasesFinalizer' -count=1`
  通过。
- A616 将 operation audit artifact 身份字段门禁上移：
  A615 在 archiver 调 executor 前阻断异常 namespace/name/UID，但不可变 audit
  artifact 自身仍会接受这些字段，只要非空即可。旧对象、fake-client、离线 artifact
  writer 或手工审计文件仍可能写出带 `/`、`.`、`..`、空白、控制字符或无效 UTF-8 的
  operation 身份，后续 release/归档链路再 fail closed，审计制品本身已经被污染。
  现在 `operationaudit.Artifact.Validate` 要求 namespace、name、UID 都是单段
  key-safe 身份；`operationauditbuilder.FromOperation` 自动复用该门禁，archiver
  的 executor-before-fail 断言也调整为更早的 artifact validator 失败。回归：
  `go test ./hack/production/operationaudit ./hack/production/internal/operationauditbuilder ./hack/production/internal/operationarchiver -run 'TestOperationAuditRejectsUnsafeIdentityFields|TestFromOperationRejectsUnsafeIdentityMetadata|TestArchiveProcessorRejectsUnsafeOperationIdentityBeforeExecutor|TestArchiveProcessorUsesStableIdentityAndReleasesFinalizer' -count=1`
  通过。
- A617 对齐 operation audit archive receipt 的 operation identity/type 门禁：
  A616 已让 audit artifact 拒绝异常 namespace/name/UID，但 artifact 的
  `operation_id`、`instance` 以及 archive receipt 的 `operation_id`、`operation_uid`、
  `instance`、`operation_type` 仍存在只校验非空或任意字符串的路径。旧 executor、
  手工 release 或离线校验工具可能生成单独看似合法、但包含路径分隔、空白、控制字符
  或未知 operation type 的 receipt，等到与 artifact 比对时才失败。现在 artifact 与
  receipt 共用单段 key-safe identity 校验，operation type 也共用白名单。回归：
  `go test ./hack/production/operationaudit ./hack/production/internal/operationauditbuilder ./hack/production/internal/operationauditrelease ./hack/production/internal/operationarchiver -run 'TestOperationAuditRejectsUnsafeIdentityFields|TestArchiveReceiptRejectsUnsafeOperationIdentity|TestFromOperationRejectsUnsafeIdentityMetadata|TestReleaseVerifiesArchiveAndRemovesOnlyAuditFinalizer|TestArchiveProcessorUsesStableIdentityAndReleasesFinalizer' -count=1`
  通过。
- A618 保护 operation audit archive 注解不可覆盖：
  `ReleaseWithExpectedReceipt` 在 audit finalizer 仍存在时会直接写入 receipt SHA、
  artifact SHA 和 version ID 注解；如果旧对象已带同名但不同值的注解，旧逻辑会覆盖，
  与不可变审计链语义冲突。生产 admission 也只要求移除 finalizer 时携带格式正确的三枚
  注解，没有要求已有 archive 注解等值保留。现在 release 代码在发现冲突注解时 fail
  closed 且不发 Update；`kubebrain-operation-audit-admission` 同步要求旧对象上已有的
  三枚 archive 注解在 finalizer release 更新中保持完全相同。回归：
  `go test ./hack/production/internal/operationauditrelease ./deploy/production ./hack/production -run 'TestReleaseRejectsConflictingArchiveAnnotations|TestOperationSubmitterApproverAndAuditAdmissionFenceHighRiskChanges|TestOperationAuditAdmissionRequiresFinalizerAndReleaseEvidence' -count=1`
  通过。
- A619 持续保护 operation audit archive evidence 注解：
  A618 保护了 finalizer release 当次更新，但生产 admission 没有像 approval evidence
  一样对 archive evidence 做独立不可变校验。audit finalizer 已释放后，仍拥有
  `kubebrainoperations` update 权限的控制器或手工修复流程可能删除或改写
  `audit-receipt-sha256`、`audit-artifact-sha256`、`audit-version-id`，破坏 CR 上的
  Object Lock 定位证据。现在 admission 增加独立规则：旧对象上任一 archive evidence
  注解存在时，后续 UPDATE 必须保留该注解且值完全相同；finalizer release 规则仍保留
  terminal/archiver 身份和格式校验。回归：
  `go test ./deploy/production ./hack/production -run 'TestOperationSubmitterApproverAndAuditAdmissionFenceHighRiskChanges|TestOperationAuditAdmissionRequiresFinalizerAndReleaseEvidence' -count=1`
  通过。
- A620 禁止 Operation 创建时预置 archive evidence：
  A619 让旧对象上的 archive evidence 注解持续不可变，但 CREATE 阶段仍只禁止预置
  approval evidence，不禁止提交者在新 Operation 上携带 `audit-receipt-sha256`、
  `audit-artifact-sha256` 或 `audit-version-id`。这些伪证据一旦创建就会被 A619
  锁住，后续真实 Object Lock 归档 release 因注解冲突 fail closed，造成可避免的
  终态阻塞。现在 admission 新增 `operations cannot be created pre-archived` 规则：
  新 Operation 不得携带任一 archive evidence 注解；只有 archiver 在终态 release
  finalizer 时能写入真实证据。回归：
  `go test ./deploy/production ./hack/production -run 'TestOperationSubmitterApproverAndAuditAdmissionFenceHighRiskChanges|TestOperationAuditAdmissionRequiresFinalizerAndReleaseEvidence' -count=1`
  通过。
- A621 禁止 UPDATE 阶段提前引入 archive evidence：
  A620 关闭了 CREATE 预置 archive evidence，但普通 UPDATE 仍可在不释放 audit
  finalizer 的情况下新增 `audit-receipt-sha256`、`audit-artifact-sha256` 或
  `audit-version-id`。这类伪证据会被 A619 的不可变规则锁住，导致真实 archiver release
  因证据冲突失败。现在 admission 新增独立门禁：UPDATE 只能保留已有 archive evidence，
  不能新增；唯一新增路径是 `kubebrain-operation-archiver` 在 Succeeded/Failed 终态移除
  `dbaas.kubebrain.io/operation-audit` finalizer，并同时提交格式正确的三枚 archive
  evidence。回归：
  `go test ./deploy/production ./hack/production -run 'TestOperationSubmitterApproverAndAuditAdmissionFenceHighRiskChanges|TestOperationAuditAdmissionRequiresFinalizerAndReleaseEvidence' -count=1`
  通过。
- A622 收紧 operation audit canonical 字符串边界：
  A616/A617 已让 audit artifact 和 archive receipt 拒绝路径穿越、空白/控制字符等
  key-safe 身份问题，但 identity 长度仍未对齐 KubeBrainOperation CRD，上游旧对象或
  fake-client 路径可携带超长 namespace/name/UID、operation ID 或 instance 进入
  canonical artifact/receipt；同时 `tenant`、`requestedBy`、`owner`、`message` 这些
  审计可读字段没有拒绝无效 UTF-8 或控制字符，`encoding/json` 会把坏 UTF-8 替换成
  U+FFFD，污染离线审计证据。现在 artifact validator 对 metadata/spec identity 增加
  CRD 对齐的长度上限，tenant 复用 DNS label 形状，requester/owner/message 要求有效
  UTF-8 且无控制字符；receipt 的 operation identity 也同步继承长度边界。
  `operationauditbuilder.FromOperation` 因复用 validator 自动 fail closed。回归：
  `go test ./hack/production/operationaudit ./hack/production/internal/operationauditbuilder -run 'TestOperationAuditRejects(NonterminalAndInvalidReceipt|UnsafeIdentityFields|UnsafeDisplayFields)|TestArchiveReceiptRejectsUnsafeOperationIdentity|TestFromOperation(RejectsUnsafeIdentityMetadata|RejectsUnsafeAuditDisplayFields|BindsImmutableSpecAndTerminalStatus)' -count=1`
  通过。
- A623 将 operation audit 字符串边界前移到 queue/API 写入端：
  A622 让归档端拒绝异常 requester/owner/message，但 `Queue.Submit` 仍可能接受认证主体
  `requestedBy` 中的控制字符或无效 UTF-8，`Claim`/`Heartbeat`/`Requeue`/`Finish`
  也只检查 owner/message 长度。这样正常 queue 写入就可能创建未来
  `operationauditbuilder.FromOperation` 必然拒绝的终态对象，导致 audit finalizer 无法释放。
  现在 queue 对 requester、status owner 和 status message 统一执行有效 UTF-8、rune
  长度上限和无控制字符校验；operation API 通过既有 `ErrInvalidSpec` 分类把坏认证主体
  请求返回 400，且不创建 Operation。回归：
  `go test ./hack/production/internal/operationqueue ./hack/production/internal/operationapi ./hack/production/operationaudit ./hack/production/internal/operationauditbuilder -run 'TestQueueRejectsSpecDriftAndInvalidCompletion|TestHandlerReturnsBadRequestForQueueSchemaValidation|TestOperationAuditRejectsUnsafeDisplayFields|TestFromOperationRejectsUnsafeAuditDisplayFields' -count=1`
  通过。
- A624 收紧 operation audit startedAtUnixNano 一致性：
  queue 正常 claim 会同时写入秒级 `startedAtUnix` 和纳秒级 `startedAtUnixNano`，但
  A622 前后的 artifact validator 只检查秒级 start/complete 关系。旧对象、旧 CRD 或
  fake-client 路径可以把负数、前一秒或后一秒的 `startedAtUnixNano` 写入 canonical
  audit artifact；离线排序和排障会看到与秒级字段矛盾的开始时间。现在
  `startedAtUnixNano` 保持可选兼容；一旦存在，必须为正且落在 `startedAtUnix` 对应的
  秒内。`operationauditbuilder.FromOperation` 因复用 validator 自动 fail closed。回归：
  `go test ./hack/production/operationaudit ./hack/production/internal/operationauditbuilder -run 'TestOperationAuditRejects(NonterminalAndInvalidReceipt|InconsistentStartedAtUnixNano)|TestFromOperation(BindsImmutableSpecAndTerminalStatus|RejectsInconsistentStartedAtUnixNano)' -count=1`
  通过。
- A625 对齐 operation audit maxAttempts 上限：
  `Queue.Submit` 已按 KubeBrainOperation CRD 拒绝 `spec.maxAttempts > 100`，但
  `operationaudit.Artifact.Validate` 只要求 `max_attempts > 0`。旧对象、旧 CRD 或
  fake-client 路径仍可生成生产 CRD 不会接受的 terminal audit artifact，并把异常 attempt
  预算写入不可变审计链。现在 artifact validator 同步拒绝超过 100 的 max attempts；
  `operationauditbuilder.FromOperation` 自动继承该门禁。回归：
  `go test ./hack/production/operationaudit ./hack/production/internal/operationauditbuilder -run 'TestOperationAuditRejectsNonterminalAndInvalidReceipt|TestFromOperationRejectsMaxAttemptsBeyondCRDLimit|TestFromOperationBindsImmutableSpecAndTerminalStatus' -count=1`
  通过。
- A626 对齐 operation audit archive receipt 的对象大小上限：
  operation audit artifact/receipt 本地读取均最多接受 1 MiB，但 archive receipt
  schema 只要求 `object_bytes > 0`。离线或暂存 receipt 因而可以声明超过 reader
  上限、生产归档链实际不可能接受的 object size；虽然 release exact-match 会在绑定
  artifact status 时发现差异，独立 receipt 校验仍应提前 fail closed。现在
  `ArchiveReceipt.Validate` 同步拒绝 `object_bytes > 1 MiB`，保留等于上限的边界值。
  回归：
  `go test ./hack/production/operationaudit -run 'TestArchiveReceiptRejectsImpossibleObjectBytes|TestOperationAuditReadersRejectOversizedJSON' -count=1`
  通过。
- A627 收紧 operation audit archive receipt 的时间因果校验：
  release finalizer 路径已经同时读取 terminal audit artifact 和 archive receipt，但旧逻辑
  只检查 digest/object/retention 绑定，没有要求 `archived_at_unix` 不早于
  `completed_at_unix`。被替换或手工暂存的 receipt 可以声明“operation 完成前已归档”
  且仍移除 audit finalizer，破坏不可变审计时间线。现在
  `ReleaseWithExpectedReceipt` 在释放 finalizer 前拒绝早于终态完成时间的归档 receipt；
  retry/idempotent 路径继续要求注解与同一 receipt/artifact/version 精确匹配。回归：
  `go test ./hack/production/internal/operationauditrelease -run 'TestReleaseRejectsReceiptAndCurrentOperationDrift' -count=1`
  通过。
- A628 将 operation audit 文本控制字符门禁同步到生产 CRD：
  A622/A623 已让 audit artifact、queue 和 API 拒绝 `requestedBy`、`owner`、`message`
  中的控制字符，但生产 KubeBrainOperation CRD 仍只限制这些字段长度。直接写 CRD、
  旧 admission 或 fake-client 路径仍可能创建未来 artifact validator 必然拒绝的终态对象，
  让 audit finalizer 长期保留。现在 CRD schema 对 `spec.requestedBy`、
  `status.owner` 和 `status.message` 增加 C0/C1 control-free pattern，提前对齐
  不可变审计 artifact 的字符串契约。回归：
  `go test ./deploy/production -run 'TestOperationCRDAndWorkerRBACFencePersistentTasks' -count=1`
  通过。
- A629 对齐 Operation 终态 CRD 与 audit artifact 必需字段：
  CRD 旧规则只要求 terminal phase 有 `completedAtUnix` 且无 lease，并只对 Succeeded
  要求 receipt 长度。直接 status update、旧 CRD 或 fake-client 路径仍可创建缺少
  `owner`/`attempt`/`observedGeneration`/`startedAtUnix`、`completedAtUnix` 早于
  `startedAtUnix`、Failed 携带 receipt，或 `startedAtUnixNano` 与秒级 start
  不一致的终态对象；这些对象都会在 `operationauditbuilder.FromOperation` 阶段失败，
  导致 audit finalizer 无法释放。现在生产 CRD 用 CEL 同步拒绝这些终态漂移；同时
  queue 的 maxAttempts exhaustion 路径会补齐 audit-complete failed status，Failed
  终态显式清空 receipt，避免“尝试次数耗尽”本身生成不可归档对象。回归：
  `go test ./hack/production/internal/operationqueue ./deploy/production -run 'TestQueue(StopsAfterMaximumAttempts|MarksRequeuedExhaustionWithAuditCompleteStatus)|TestOperationCRDAndWorkerRBACFencePersistentTasks' -count=1`
  通过。
- A630 对齐 operation audit 的 CRD 标识符字符集：
  `Queue.Submit` 与生产 CRD 已要求 `spec.operationID`/`spec.instance` 满足
  `^[A-Za-z0-9][A-Za-z0-9._-]*$`，但 `operationaudit.Artifact.Validate` 与 archive
  receipt 仍只排除空白、控制字符、`/` 和 `.`/`..`，会接受 `backup:1`、
  `instance@a` 或非 ASCII 等真实 CRD 不会接受的标识符。旧对象、旧 CRD、fake-client
  或被替换的 executor 可以把这类标识符写入不可变 audit artifact/receipt，并让离线证据
  与生产 API 契约分叉。现在 artifact 与 archive receipt 对 operation ID 和 instance
  复用 CRD/queue 的 ASCII 标识符形状；namespace/name/UID 仍按 Kubernetes metadata
  证据的 key-safe 规则处理。回归：
  `go test ./hack/production/operationaudit ./hack/production/internal/operationauditbuilder -run 'TestOperationAuditRejectsUnsafeIdentityFields|TestArchiveReceiptRejectsUnsafeOperationIdentity|TestFromOperationRejectsUnsafeIdentityMetadata' -count=1`
  通过。
- A631 对齐 operation audit artifact 的 Kubernetes metadata 形状：
  A630 收紧了 `spec.operationID`/`spec.instance`，但 artifact 的 `namespace`/`name`
  仍只做 key-safe 校验。真实 apiserver 要求 namespace 是 DNS label、Operation
  metadata.name 是 DNS subdomain；旧对象、旧 CRD 或 fake-client 路径仍可把
  `Operations`、`tenant.a` 或 `backup:1` 写入 terminal audit artifact，并让离线
  证据描述生产 API 不可能接受的对象。现在 artifact validator 对 namespace/name
  分别执行 Kubernetes metadata 形状校验，`operationauditbuilder.FromOperation`
  自动继承该 fail-closed 行为；UID 仍按已有 key-safe evidence 规则处理。回归：
  `go test ./hack/production/operationaudit ./hack/production/internal/operationauditbuilder -run 'TestOperationAuditRejectsUnsafeIdentityFields|TestFromOperationRejectsUnsafeIdentityMetadata' -count=1`
  通过。
- A632 收紧 operation audit receipt 匹配 helper：
  `operationaudit.ArchiveReceipt.Matches` 是公开 helper，但旧实现只做字段等值比较；
  调用方若绕过 `Inspect`/`InspectArchiveReceipt`，全零 receipt 与全零 status 也会返回
  matched。当前 release 路径已经先执行 Inspect，不受影响，但公开 API 不应允许未来路径
  误把未验证证据当作已绑定的 Object Lock receipt。现在 `Matches` 先要求 archive receipt、
  terminal artifact、artifact SHA-256 和 artifact size 都满足同一 schema，再比较 operation
  identity、phase、execution receipt、artifact digest 和字节数；非法 receipt/status 以及
  合法但字段漂移的 receipt 均返回 false。回归：
  `go test ./hack/production/operationaudit ./hack/production/internal/operationauditrelease -run 'TestArchiveReceiptMatchesRequiresValidatedEvidence|TestReleaseRejectsReceiptAndCurrentOperationDrift|TestReleaseVerifiesArchiveAndRemovesOnlyAuditFinalizer' -count=1`
  通过。
- A633 统一 Object Lock executor 的 operation audit receipt schema：
  A632 关闭了 release 侧匹配 helper 的未验证输入问题，但
  `hack/backup/objectstore/internal/objectstore.AuditReceipt.Validate` 仍保留较旧的宽松
  schema，只检查 operation identity 非空、scope 非空和 digest 形状。直接调用或替换的
  Object Lock executor 可能先写出 `backup:1`、`Backup/Deletion`、带空白 scope、
  路径穿越 object key 或超过 audit reader 上限的 receipt，随后 release 端再 fail closed，
  已经扩大了外部对象写入和排障面。现在 executor 写入/读取 audit receipt 时直接复用
  `operationaudit.ArchiveReceipt.Validate`，与 release、offline inspect 和 operation audit
  artifact 的 receipt 契约完全一致。回归：
  `(cd hack/backup/objectstore && go test ./internal/objectstore -run 'TestAuditReceiptUsesOperationAuditArchiveSchema|TestReceiptsRejectNonLowercaseArtifactDigest|TestReceiptReadersRejectAmbiguousJSON|TestArchiveAuditUploadsVerifiesAndRetriesExactVersion|TestBuildInventoryManifest' -count=1)`
  通过。
- A634 对齐 immutable blob receipt 对象大小边界：
  `ArchiveBlob` 本地上传最多接受 16 MiB，`ReadBlob` 也拒绝远端对象超过同一上限，
  但 `BlobReceipt.Validate` 和 `BlobReadReceipt.Validate` 仍只要求 `object_bytes > 0`。
  手工暂存、离线读取或替换 executor 因而可以写出生产 executor 不可能合法生成的 oversized
  immutable blob receipt，再把异常 size 传给后续 billing/metering 读回路径。现在 archive
  与 read 两类 immutable blob receipt validator 都拒绝 `object_bytes > maxImmutableBlobBytes`，
  保留等于 16 MiB 的合法边界，与实际 upload/read 上限一致。回归：
  `(cd hack/backup/objectstore && go test ./internal/objectstore -run 'TestImmutableBlobReceiptsRejectImpossibleObjectBytes|TestArchiveBlobRejectsEmptyOversizedAndNonCanonicalReceipt|TestReadBlobRequiresOneProtectedExactVersion|TestReceiptsRejectNonLowercaseArtifactDigest' -count=1)`
  通过。
- A635 收紧 object usage 版本数与字节数一致性：
  `MeasureUsage` 对每个远端版本都要求 `object_bytes > 0`，因此合法 usage receipt 只能是
  `remote_versions == 0 && total_object_bytes == 0`，或二者同时为正。旧
  `UsageReceipt.Validate` 只拒绝负数，生产 `Snapshot.Validate` 也只拒绝“0 个版本但非 0
  字节”的一侧；手工暂存或替换 executor 可写出“有版本但总字节为 0”的不可生成计量证据，
  进入后续 storage sample/rollup。现在 objectstore usage receipt 与生产 storage sample
  都要求版本数和总字节数同为零或同为正。回归：
  `(cd hack/backup/objectstore && go test ./internal/objectstore -run 'TestUsageReceiptRejectsImpossibleVersionByteTotals|TestMeasureUsageVerifiesAndTotalsEveryVersion|TestUsageReceiptRejectsTamperAndNonCanonicalJSON' -count=1)`；
  `go test ./hack/production/internal/meteringstorage -run 'TestSnapshot(RejectsImpossibleVersionByteTotals|RejectsInvalidEvidenceAndSlot|CanonicalRoundTrip)|TestArchiverRejectsUsageReceiptMismatchBeforeArchive' -count=1`
  通过。
- A636 固定空 object usage 的版本清单 digest：
  A635 让 usage receipt/storage sample 的版本数和总字节数同为零或同为正，但空 usage
  仍可携带任意小写 64 位 `versions_sha256`。真实 `MeasureUsage` 对空版本列表的
  canonical 输入固定为 `[]\n`，digest 唯一；手工暂存或替换 executor 可伪造空计量证据，
  并让后续 sample/rollup 无法区分空清单来源。现在 objectstore usage receipt 与生产
  storage sample 在 `remote_versions == 0` 时都要求 `versions_sha256 == sha256("[]\n")`；
  非空 usage 继续只校验 digest 形状，因为 standalone validator 无法从 receipt 内部重算
  版本列表。回归：
  `(cd hack/backup/objectstore && go test ./internal/objectstore -run 'TestMeasureUsageVerifiesAndTotalsEveryVersion|TestMeasureUsageUsesCanonicalEmptyVersionsDigest|TestUsageReceiptRejectsImpossibleVersionByteTotals' -count=1)`；
  `go test ./hack/production/internal/meteringstorage -run 'TestSnapshotRejectsImpossibleVersionByteTotals|TestSnapshotCanonicalRoundTrip' -count=1`
  通过。
- A637 拒绝 object storage rollup 的非整小时 byte-seconds：
  `BuildRollup` 只会把 24 个小时样本的 `total_object_bytes` 分别乘以 3600 后求和，因此
  合法 `object_storage_byte_seconds` 必须是 3600 的整数倍，0 用量仍然合法。旧
  `Rollup.Validate` 只拒绝负数，手工暂存或替换 rollup executor 可写出 `... + 1` 这类
  不可由 hourly sample 生成的 fractional-hour 计量量，并在后续 archive/settlement
  中被当作完整日聚合读取。现在 rollup validator 与写入路径都拒绝非 3600 倍数的
  byte-seconds。回归：
  `go test ./hack/production/internal/meteringstorage -run 'TestStorageRollupRejectsFractionalSlotByteSeconds|TestBuildStorageRollupIntegratesTwentyFourSnapshots|TestBuildStorageRollupFailsClosed' -count=1`
  通过。
- A638 统一 production usage receipt reader 与 snapshot schema：
  A635/A636 已收紧 `Snapshot.Validate`，但 `meteringstorage.readUsageReceipt` 仍保留旧的
  宽松内联校验，stdout parser 甚至只解 JSON 不校验 schema。非法 usage receipt 最终会在
  `BuildSnapshot` 处 fail closed，但本地 reader/stdout 入口会先接受不可由 objectstore
  生成的证据，扩大排障面并增加未来调用方绕过 BuildSnapshot 的风险。现在
  `UsageReceipt.Validate` 成为 production meteringstorage 内部唯一 usage receipt schema，
  文件 reader、stdout parser 与 `BuildSnapshot` 都复用它，覆盖格式列表、版本/字节一致性
  和空清单 digest。回归：
  `go test ./hack/production/internal/meteringstorage -run 'TestUsageReceiptReadersUseSnapshotSchema|TestArchiverRejectsUsageReceiptMismatchBeforeArchive|TestArchiverMeasuresBeforeArchivingAndIsDeterministic' -count=1`
  通过。
- A639 对齐 object storage metering writer 的新输出大小边界：
  A520 已让 object usage receipt、hourly storage sample 和 daily storage rollup 的 reader
  拒绝超过 1 MiB 的本地 JSON，writer 也会在比较 oversized 既有文件时 fail closed；但
  `WriteSnapshotAtomic`/`WriteRollupAtomic` 在写入新 artifact 前没有检查 marshal 后的
  canonical JSON 大小。直接调用 writer 可构造 schema 合法但字符串字段超大的 sample/rollup，
  让 writer 先 link 出一份随后 `ReadSnapshot`/`ReadRollup` 读不回来的 oversized 文件。
  现在两个 writer 在创建临时文件前先拒绝超过 `maxMeteringStorageJSONBytes` 的新输出，
  失败时目标路径保持不存在。回归：
  `go test ./hack/production/internal/meteringstorage -run 'TestSnapshotWriterRejectsOversizedNewOutputBeforeLink|TestStorageRollupWriterRejectsOversizedNewOutputBeforeLink|TestSnapshotRejectsOversizedInputs|TestStorageRollupRejectsOversizedInputs' -count=1`
  通过。
- A640 对齐 resource metering writer 的新输出大小边界：
  A521 已为 resource metering sample/rollup reader 设置 1 MiB 上限，但 writer 仍只在
  目标路径已存在且 oversized 时 fail closed。`meteringarchive.Rollup` 的 source object key
  来自对象执行器证据，直接调用 writer 可构造 schema 合法但超大 key 的 rollup，使新文件
  link 后再被 `ReadRollup` 拒绝。现在 `WriteAtomic` 与 `WriteRollupAtomic` 在生成 canonical
  JSON 后先检查 `maxMeteringArchiveJSONBytes`，再创建临时文件；回归覆盖 oversized 新 rollup
  失败后目标路径不存在。回归：
  `go test ./hack/production/internal/meteringarchive -run 'TestRollupWriterRejectsOversizedNewOutputBeforeLink|TestRollupRejectsOversizedInputs|TestSampleRejectsOversizedInputs' -count=1`
  通过。
- A641 对齐 metering billing writer 的新输出大小边界：
  price catalog、charge 与 settlement artifact reader 分别有 1 MiB/1 MiB/4 MiB 本地 JSON
  上限，但共享 `writeCanonicalAtomic` 旧实现只检查 oversized 既有文件；直接调用
  `WriteChargeAtomic` 可通过超长 source object key 构造 schema 合法但超过 reader 上限的新
  charge，并在 link 后留下不可读 artifact。现在 shared writer helper 先按调用方传入的
  reader 上限拒绝新输出，再创建临时文件；catalog、charge 和 settlement writer 都继承该
  fail-closed 行为。回归：
  `go test ./hack/production/internal/meteringbilling -run 'TestChargeWriterRejectsOversizedNewOutputBeforeLink|TestChargeRejectsOversizedInput|TestPriceCatalogRejectsOversizedInputs|TestSettlementRejectsOversizedInputs' -count=1`
  通过。
- A642 让本地 metering/billing artifact writer 在并发同内容 link 时幂等：
  生产 CronJob、补偿任务或手工重跑可能并发生成同一个 hourly sample、daily rollup、price
  catalog、charge 或 settlement artifact。旧 writer 在创建临时文件前会先读取目标文件；
  若另一个同内容 writer 恰好在预读之后、`os.Link` 之前完成 link，当前 writer 会直接返回
  `EEXIST`，而不是复用已存在的 canonical artifact。Object Lock receipt writer 已把同内容
  `EEXIST` 视为幂等成功，metering/storage/resource/billing 本地 writer 现在也对齐该语义：
  link 返回 `EEXIST` 时重新读取目标文件，内容一致则成功，不一致仍拒绝覆盖。回归：
  `go test ./hack/production/internal/meteringstorage ./hack/production/internal/meteringarchive ./hack/production/internal/meteringbilling -run 'TestSnapshotWriterTreatsConcurrentIdenticalLinkAsIdempotent|TestSampleWriterTreatsConcurrentIdenticalLinkAsIdempotent|TestCanonicalWriterTreatsConcurrentIdenticalLinkAsIdempotent' -count=1`
  通过。
- A643 让 operation audit artifact writer 在并发同内容 link 时幂等：
  operation audit archiver 也会把 terminal CRD status 先冻结成本地 canonical artifact，再交给
  Object Lock executor。普通重复调用已有幂等测试，但旧 `operationaudit.WriteAtomic` 在预读
  目标文件后、`os.Link` 前若遇到另一个同内容 writer 抢先完成 link，会直接返回 `EEXIST`，
  让可重试 archiver/补偿任务误判失败并延迟 audit finalizer 释放。现在 audit artifact writer
  与 A642 的 metering/billing 本地 writer、Object Lock receipt writer 一致：同内容
  `EEXIST` 视为成功，内容漂移仍拒绝覆盖。回归：
  `go test ./hack/production/operationaudit -run 'TestOperationAuditArtifactWriterTreatsConcurrentIdenticalLinkAsIdempotent|TestOperationAuditArtifactIsCanonicalAndNonOverwriting' -count=1`
  通过。
- A644 让 immutable blob read output writer 在并发同内容 link 时幂等：
  A642/A643 对齐了生产 metering、billing 与 audit 的本地 artifact writer，但 Object Lock
  executor 的 `writeBlobOutputAtomic` 仍在预读后、`os.Link` 前遇到同内容输出被并发创建时
  返回 `EEXIST`。`ReadBlob` 会把 exact-version immutable blob 下载到本地，供 sample、
  rollup、charge 和 operation release 校验；并发补偿或重试同一 exact version 时，已存在
  相同 bytes 应视为成功。现在 immutable blob output writer 在 `EEXIST` 时 bounded read
  目标文件，内容一致则成功，漂移仍拒绝覆盖。回归：
  `go test ./internal/objectstore -run 'TestReadBlobOutputWriterTreatsConcurrentIdenticalLinkAsIdempotent|TestReadBlobRequiresOneProtectedExactVersion|TestArchiveBlobRejectsEmptyOversizedAndNonCanonicalReceipt' -count=1`
  在 `hack/backup/objectstore` 模块内通过。
- A645 对齐 Object Lock JSON writer 的新输出大小边界：
  objectstore 模块的 receipt、usage receipt、inventory manifest/receipt reader 已统一拒绝超过
  1 MiB 的本地 JSON，但 `WriteReceiptAtomic` 和共享 `writeJSONAtomic` 旧实现只在目标文件
  已存在且 oversized 时 fail closed；直接调用 writer 可用超长 object key/prefix 构造 schema
  合法但超过 reader 上限的新 JSON，link 后留下无法读回、无法幂等复用的证据文件。现在
  backup/deletion/audit/blob/usage/inventory writer 都在创建临时文件前检查 canonical JSON
  长度，超过 `maxObjectStoreJSONBytes` 时目标路径保持不存在。回归：
  `go test ./internal/objectstore -run 'TestObjectStoreJSONWritersRejectOversizedNewOutputBeforeLink|TestObjectStoreJSONReadersRejectOversizedInput|TestWriteReceiptAtomicIsIdempotentAndNonOverwriting' -count=1`
  在 `hack/backup/objectstore` 模块内通过。
- A646 让 post-restore audit receipt 发布竞争幂等：
  A190 的 `audit-restored-instance.sh` 已能在重试已有 receipt 时重新执行在线拓扑和事务探针，
  但两个 worker 若同时完成完整审计，后完成者在预检查之后、硬链接发布之前遇到另一个
  worker 抢先发布的同一操作 receipt，会直接把 `EEXIST` 当覆盖冲突失败。现在最终发布
  遇到硬链接冲突时复用同一套 exact key set、digest、拓扑、探针 revision 与时间因果
  校验；只有现有 receipt 严格绑定当前 audit operation 才视为幂等成功，漂移或 unknown
  field 仍 fail closed。回归：
  `go test ./hack/production -run 'TestPostRestoreAuditTreatsConcurrentReceiptPublishAsIdempotent|TestPostRestoreAuditPublishesReceiptAndRechecksOnRetry|TestPostRestoreAuditRejectsExistingReceiptWithUnknownFields' -count=1`
  通过。
- A647 让 restore cutover receipt 发布竞争幂等：
  `switch-restore-traffic.sh` 的 complete 阶段会在切流、EndpointSlice 和 public data
  复核后发布 `kubebrain.restore-cutover.receipt.v1`。旧逻辑只在生成 receipt 前检查
  已有终态文件；若另一个 worker 在检查之后、硬链接发布之前抢先写入同一操作的有效
  receipt，当前 worker 会因为 `completed_at_unix` 不同而把 `EEXIST` 当作覆盖冲突。
  现在 receipt 校验被抽成共享 helper，最终 `atomic_publish` 仅在目标是 cutover receipt
  且 existing receipt 严格绑定 operation、Service UID、state digest、source/target、
  snapshot revision、replica 数和三项成功标志时视为幂等成功；state/marker 仍保留
  字节级幂等，receipt unknown field 或漂移仍 fail closed。回归：
  `go test ./hack/production -run 'TestRestoreTrafficCutoverTreatsConcurrentReceiptPublishAsIdempotent|TestRestoreTrafficCutoverLifecycleAndRollback|TestRestoreTrafficCutoverRejectsExistingReceiptWithUnknownFields' -count=1`
  通过。
- A648 让 instance destroy receipt 发布竞争幂等：
  `destroy-instance.sh` 的 complete 阶段会在 quiesce/destroy marker、UID fence、
  PVC/TiDB/KubeBrain residue 全部复核后发布 `kubebrain.destroy.receipt.v1`。旧逻辑与
  A647 类似：生成 receipt 前若未看到终态文件，但另一个 worker 在硬链接前抢先发布
  同一操作的有效 receipt，当前 worker 会因 `completed_at_unix` 不同把 `EEXIST`
  当作覆盖冲突。现在 `atomic_publish` 仅在目标是 destroy receipt，且当前 complete
  已从冻结 state 绑定 backup SHA/revision 时，复用既有 exact key set 校验；existing
  receipt 必须匹配 instance、operation、namespace、TiDB cluster、backup SHA/revision
  和 `resources_absent=true` 才视为幂等成功。state/quiesced/destroyed 证据仍保持
  字节级幂等，receipt 漂移或嵌套伪造字段仍 fail closed。回归：
  `go test ./hack/production -run 'TestDestroyInstanceTreatsConcurrentReceiptPublishAsIdempotent|TestDestroyInstanceLifecycleIsRetrySafe|TestDestroyInstanceRejectsNestedExistingReceiptFields' -count=1`
  通过。
- A649 让 certificate rotation receipt 发布竞争幂等：
  `validate-certificate-rotation.sh` 的 complete 阶段在确认新凭据健康、旧凭据被拒绝、
  Pod UID/readiness/restart 未漂移后发布 `kubebrain.certificate-rotation.receipt.v1`。
  旧逻辑只在生成 receipt 前检查已有终态文件；若另一个 worker 在检查后、硬链接前
  发布同一 rotation 的有效 receipt，当前 worker 会因时间戳不同把 `EEXIST` 当覆盖
  冲突。现在 `atomic_publish` 仅在目标是 rotation receipt，且 complete 已绑定
  old/new certificate SHA-256 fingerprint 时复用 exact key set 校验；existing receipt
  必须匹配 instance、rotation、endpoint、replica 数、old/new fingerprint、
  `pods_unchanged=true` 和 `old_certificate_rejected=true` 才视为幂等成功。
  begin/overlap 证据仍保持字节级幂等，receipt 漂移或嵌套伪造字段仍 fail closed。回归：
  `go test ./hack/production -run 'TestValidateCertificateRotationTreatsConcurrentReceiptPublishAsIdempotent|TestValidateCertificateRotationLifecycle|TestValidateCertificateRotationRejectsNestedExistingReceiptFields' -count=1`
  通过。
- A650 让 boundary cleanup receipt 发布竞争幂等：
  `cleanup-instance-boundaries.sh` 的 complete 阶段在 destroy receipt、冻结 namespace/secret
  UID fence 和资源 absent 全部复核后发布 `kubebrain.boundary-cleanup.receipt.v1`。旧逻辑
  与 A648/A649 相同，只在生成前检查已有终态文件；若另一个 worker 在 hard-link 前
  发布同一 cleanup 的有效 receipt，当前 worker 会因 `completed_at_unix` 不同误判覆盖。
  现在 receipt 校验被抽成共享 helper，`atomic_publish` 仅在目标是 cleanup receipt 且
  complete 已从冻结 state 构造 `credential_secrets` JSON 时复用 exact key set 校验；
  existing receipt 必须匹配 cleanup ID、destroy receipt SHA、三个 namespace 字段、
  credential secret 列表以及 absent 标志才视为幂等成功。boundary state 仍保持字节级
  幂等，receipt unknown field 或漂移仍 fail closed。回归：
  `go test ./hack/production -run 'TestBoundaryCleanupTreatsConcurrentReceiptPublishAsIdempotent|TestBoundaryCleanupLifecycleIsRetrySafe|TestBoundaryCleanupRejectsExistingReceiptWithUnknownFields' -count=1`
  通过。
- A651 补齐 backup deletion operation receipt 发布竞争回归：
  `run-backup-deletion-operation.sh` 的 workflow-level
  `kubebrain.backup-deletion-operation.receipt.v1` 已在 existing receipt 和 hard-link
  冲突路径复用 strict `validate_operation_receipt`，但旧测试只覆盖已有 receipt 漂移，
  没有固定“生成新 receipt 后、link 前被另一个 worker 发布”的窗口；同时冲突 receipt
  漂移时手写发布块会在退出前留下 `.backup-deletion-receipt.*` 临时文件。现在发布逻辑
  收敛到 `publish_operation_receipt` helper：并发等价 receipt 通过同一 exact key set
  和 source/pre/deletion/post SHA 绑定校验后幂等成功，漂移继续 fail closed，所有 link
  冲突分支都会清理 0600 临时文件并同步目录。回归：
  `go test ./hack/production -run 'TestBackupDeletionOperation' -count=1`
  通过。
- A652 收紧 operation runner receipt digest 提交前复检：
  Destroy、CertificateRotation、RestoreCutover 和 PostRestoreAudit runner 都会在子状态机
  完成后用 strict validator 复核 receipt，再把 `sha256sum` 写入 Operation 终态；旧逻辑
  在 validator 与 `sha256sum` 之间仍有本地 TOCTOU 窗口，异常替换可能让 queue 记录未验证
  字节的 digest。现在四个 runner 都通过 `validated_*_receipt_digest` helper 执行
  validate→hash→validate，第二次复检失败时沿用原来的 retry/fail 语义且不提交 succeed。
  回归用 `sha256sum` wrapper 在最终 digest 读取瞬间替换 receipt，覆盖四类 runner：
  `go test ./hack/production -run 'Test(DestroyOperation|CertificateRotationOperation|RestoreCutoverOperation|PostRestoreAuditOperation)RejectsReceiptTamperedDuringDigest' -count=1`
  通过。
- A653 补齐 backup/backup-deletion runner receipt digest 提交前复检：
  A652 已覆盖 Destroy、CertificateRotation、RestoreCutover 和 PostRestoreAudit，但 Backup
  与 BackupDeletion runner 仍在 strict receipt validator 通过后直接计算 `sha256sum`
  并提交 Operation 终态。若本地 object receipt 或 workflow-level backup deletion receipt
  在 validator 与 digest 读取之间被替换，queue 仍可能记录未验证字节的 SHA。现在
  `run-backup-operation.sh` 与 `run-backup-deletion-operation.sh` 也改为
  validate→hash→validate；第二次复检失败时保持原有 retry/失败语义，不提交 succeed。
  回归：
  `go test ./hack/production -run 'Test(BackupOperation|BackupDeletionOperation)RejectsReceiptTamperedDuringDigest' -count=1`
  通过。
- A654 收紧 backup deletion 子证据 receipt digest 汇总复检：
  A653 关闭了 workflow-level operation receipt 的最终 digest 窗口，但
  `run-backup-deletion-operation.sh` 还会把 pre-inventory、delete、post-inventory 三个
  子 receipt 的 SHA 写入 `kubebrain.backup-deletion-operation.receipt.v1`；旧逻辑在子
  receipt strict validator 通过后直接读取 `sha256sum`，本地文件若在读取瞬间被替换，
  operation receipt 会绑定未验证的子证据 digest。现在三类子 receipt 都通过
  validate→hash→validate 汇总，删除 receipt 的 schema 校验也抽成共享 helper，复检失败
  沿用原有错误文案且不提交 succeed。回归：
  `go test ./hack/production -run 'TestBackupDeletionOperationRejectsEvidenceTamperedDuringDigest' -count=1`
  通过。
- A655 收紧 backup deletion source receipt 解析后 digest fence：
  BackupDeletion 参数会携带 source object backup receipt 的 SHA，runner 在开头已做一次
  digest 比对，但随后用 jq 解析 bucket/object/version 时没有再次确认本地 source receipt
  仍是同一份 bytes；若文件在参数 digest 校验后、source fields 解析前被替换成 schema
  合法但 digest 不同的 receipt，后续删除证据会出现 source SHA 与实际解析输入不一致。
  现在 source fields 提取后立即重算 source receipt SHA 并与参数 digest 比对，漂移时在
  object workflow 前失败，不提交 succeed。回归：
  `go test ./hack/production -run 'TestBackupDeletionOperationRejectsSourceReceiptTamperedDuringParse' -count=1`
  通过。
- A656 收紧 cold physical restore 本地证据稳定性：
  A459-A463 已让 `cold-restore-execute.sh` 绑定 restore manifest、target identity 和实际
  PVC/VolumeSnapshotContent inventory，但脚本仍从调用方提供的 `RECEIPT_FILE` 与
  `RESTORE_MANIFEST` 路径多次读取；若 snapshot receipt 在 validation 后被替换，最终
  restore receipt 可能记录另一份文件的 SHA；若 restore manifest 路径在 canonical compare
  后被替换，`kubectl create -f` 可能应用未绑定内容；若 `RESTORE_RECEIPT_FILE` 在开头
  `! -e` 检查后由并发进程创建，旧 `mv` 会覆盖既有证据。现在 cold restore executor
  捕获并复检 source snapshot receipt SHA，从私有 canonical manifest 副本执行 create，
  并通过 hard-link 发布 restore receipt，已存在时 fail closed 且不覆盖。回归：
  `go test ./hack/production -run 'TestColdRestoreExecute' -count=1`
  通过。
- A657 收紧 cold physical snapshot witness/receipt 证据稳定性：
  `cold-snapshot-execute.sh` 会先用 `logical-status` 校验 semantic witness，再把 witness
  文件 SHA 写入 `kubebrain.cold-physical-snapshot.v2` receipt；旧逻辑在 status 和
  `sha256sum` 之间存在本地替换窗口，可能让 receipt 中的 logical witness 状态来自旧
  bytes、`file_sha256` 来自新 bytes。同时最终 receipt 仍用 `mv` 发布，若并发进程在
  开头 `! -e` 检查后创建同名 receipt，会被覆盖。现在 executor 在 logical-status 前捕获
  witness 文件 SHA，status 后和 receipt 发布前都复检未漂移，并通过 hard-link 发布 snapshot
  receipt，已存在时 fail closed 且不覆盖。回归：
  `go test ./hack/production -run 'TestColdSnapshotExecute' -count=1`
  通过。
- A658 收紧 cold restore verifier witness/semantic receipt 证据稳定性：
  `cold-restore-verify` 通过 `backupfile.OpenVerified` 解析 semantic witness，再单独重读
  `WITNESS_FILE` 计算 `semantic_witness.file_sha256` 的绑定；若本地 witness 在验证和
  digest 之间被替换，semantic verify receipt 可能把已验证状态与另一份文件 SHA 混用。
  同时 `SEMANTIC_RECEIPT_FILE` 虽在入口要求不存在，最终 writer 仍用 rename 发布，并发
  创建同名文件时会覆盖。现在 verifier 执行 digest→OpenVerified→digest 复检，并在最终
  receipt 发布前再次确认 witness 未漂移；semantic receipt 也通过 hard-link 非覆盖发布，
  目标已存在时 fail closed。回归：
  `go test ./hack/backup/cmd/cold-restore-verify -run 'Test(OpenStableWitnessRejectsDriftDuringValidation|WriteAtomicSemanticReceipt|ValidateReceiptChain|ValidateRestoreManifestBinding|FileDigest)' -count=1`
  通过。
- A659 收紧 cold restore renderer manifest 输出发布：
  `cold-restore-render` 入口要求 `--output` 不存在，但 writer 旧实现使用 rename；若并发
  进程在入口检查后创建同名 restore manifest，最终发布会覆盖既有文件。现在 renderer
  用 hard-link 发布新 manifest，目标已存在时返回 `EEXIST` 并保留原文件，避免 restore
  executor 后续消费被竞态替换的本地 manifest。回归：
  `go test ./hack/backup/cmd/cold-restore-render -run 'TestWriteAtomic' -count=20`
  通过。
- A660 冻结 cold restore executor 的 renderer 输入 receipt：
  A656 已让 `cold-restore-execute.sh` 复检调用方 `RECEIPT_FILE` 的 SHA，但 canonical
  renderer 仍直接从该路径重读 snapshot receipt；若本地 receipt 在初始解析后、renderer
  调用前被替换，executor 可能把旧 receipt 变量、新 receipt 渲染结果和最终 digest 混用。
  现在 executor 先把 source snapshot receipt 捕获到私有 0600 副本，所有本地解析和
  `cold-restore-render` 复算都只读该副本；调用方路径仍在捕获后、变更前和签收前做
  digest 复检，漂移时 fail closed。回归：
  `go test ./hack/production -run 'TestColdRestoreExecute' -count=1`
  通过。
- A661 收紧 logical backup JSONL 制品非覆盖发布：
  `backupfile.AtomicWriter` 旧实现通过 rename 提交 `OUTPUT`，生产 backup runner 虽在调用
  exporter 前检查 artifact path 不存在，但并发进程可在检查后创建同名文件并被 exporter
  覆盖；直接运行 `logical-export.sh` 也会覆盖默认输出。现在 logical backup 主制品通过
  hard-link 发布，目标已存在时返回 `EEXIST` 并保留原文件；相关 smoke 脚本的默认
  `OUTPUT` 改为私有临时目录内尚不存在的路径，继续兼容新建制品语义。回归：
  `go test ./hack/backup/internal/backupfile -run 'Test(AtomicWriterAndVerifiedReader|CommitDoesNotOverwriteExistingBackup|AbortDoesNotReplaceExistingBackup)' -count=20`
  通过，且 `bash -n` 覆盖 logical-export 与相关 smoke 脚本。
- A662 为 Object Lock backup receipt 增加 whole-file SHA 绑定：
  `kubebrain.object-backup.receipt.v1` 旧 receipt 只记录 logical backup 的 footer SHA、
  object bytes 和远端 version；S3 `ChecksumSHA256` 已覆盖 exact JSONL bytes，但该
  whole-file digest 没写入本地 receipt/metadata，后续 runner 只能用 logical status 与
  size 复核。现在 upload workflow 计算并写入 `artifact_file_sha256`，S3 metadata 与
  远端下载复核也要求该 digest 匹配；`run-backup-operation.sh` succeed 前要求新 receipt
  的 `artifact_file_sha256` 等于当前 artifact whole-file SHA。读取旧 backup receipt 仍
  允许缺失该字段，避免历史受保护版本失去删除/审计路径；backup deletion source parser
  同时接受旧/新 key set。回归：
  `(cd hack/backup/objectstore && go test ./internal/objectstore -count=1)` 与
  `go test ./hack/production -run 'TestBackupOperation|TestBackupDeletionOperation' -count=1`
  通过。
- A663 收紧 restore cutover state 与最终 receipt 的 digest fence：
  `switch-restore-traffic.sh` 在 complete 阶段会多次读取 prepare 生成的
  `kubebrain.restore-cutover.state.v1`，旧逻辑在 schema/pod/EndpointSlice/数据复核后
  再独立读取 state 字段和 `sha256sum` 生成 `kubebrain.restore-cutover.receipt.v1`；
  若本地 state 在最终 receipt JSON 生成窗口被替换或追加，脚本可能发布与前序切流验证
  输入不一致的完成 receipt。现在 state schema validator 可组合返回，cutover/verify/
  rollback/complete 在发布 marker 或 receipt 前后都绑定同一个 state SHA；existing receipt
  复用也先冻结 state digest，再读取字段并复检未漂移。回归：
  `go test ./hack/production -run 'TestRestoreTrafficCutover|TestRestoreCutoverOperation' -count=1`
  通过。
- A664 为 post-restore audit receipt 绑定 cutover 源证据 bytes：
  `kubebrain.post-restore-audit.receipt.v1` 旧 receipt 只记录 cutover operation、service
  UID、artifact SHA 与 revision 等语义字段，未写入审计实际消费的
  `kubebrain.restore-cutover.state.v1` 和 `kubebrain.restore-cutover.receipt.v1` 的
  whole-file digest；runner 最终提交 succeed 时也只能重新从当前本地文件解析语义字段。
  现在审计 executor 对 cutover state/receipt 都执行 validate→hash→validate，并在长时间
  probe 结束、receipt JSON 生成后和并发 receipt 复用前复检未漂移；新 audit receipt 写入
  `cutover_state_sha256` 与 `cutover_receipt_sha256`，外层 operation runner 也校验这两个
  digest 后才提交 receipt SHA。旧 audit receipt 仍按 legacy key set 接受。回归：
  `go test ./hack/production -run 'TestPostRestoreAudit|TestPostRestoreAuditOperation' -count=1`
  通过。
- A665 冻结 Destroy operation 的 backup 输入 bytes：
  `run-destroy-operation.sh` 旧逻辑只在参数解析后计算一次 `backup_file_sha256`，随后把
  调用方提供的 `BACKUP_INPUT` 路径传给 `destroy-instance.sh`；若本地 backup 文件在这次
  digest 校验后、prepare 读取前被替换，破坏性 quiesce/destroy 阶段可能基于未绑定的
  backup 输入继续推进。现在 runner 在参数 digest 通过后立即把 backup 捕获到私有临时
  副本，要求副本和调用方路径的 whole-file SHA 都仍等于参数 `backup_file_sha256`，并只把
  私有副本传给 destroy executor；捕获窗口内漂移会 retry，且不会进入 prepare。回归：
  `go test ./hack/production -run 'TestDestroyOperation' -count=1`
  通过。
- A666 收紧 direct destroy executor 的 backup 状态读取：
  A665 保护了 operation runner 路径，但直接执行 `destroy-instance.sh prepare` 时，旧逻辑
  仍分别调用 `logical-status FIELD=sha256` 与 `FIELD=revision`，本地 backup 若在两次读取
  之间被替换，`kubebrain.destroy.state.v1` 可能混入不同 backup 的 SHA 与 revision。现在
  prepare 只读取一次 canonical logical status JSON，严格校验 schema 后提取 SHA/revision，
  并在 logical-status 前后、state 发布前复核 `BACKUP_INPUT` whole-file SHA 未漂移；漂移
  时 fail closed，不发布 prepare evidence。回归：
  `go test ./hack/production -run 'TestDestroyInstance|TestDestroyOperation' -count=1`
  通过。
- A667 冻结 CertificateRotation operation 的 credential 输入：
  `run-certificate-rotation-operation.sh` 旧逻辑只逐个校验 old/new/overlap CA、cert、key
  的参数 SHA，随后 gate 与 publish hook 仍通过原始 `PARAMETERS_INPUT` 或原始 credential
  路径消费本地文件；若校验后文件被替换，证书轮转可能发布未绑定 bytes。现在 runner 在
  参数 digest 通过后把 7 个 credential 捕获到私有临时目录，要求副本和调用方源路径 digest
  都仍等于参数 SHA，并生成内部参数副本把 credential 路径改写为冻结文件；gate 和 publish
  hook 都只接收该内部参数文件。捕获窗口漂移会 retry，且不会进入 begin gate。回归：
  `go test ./hack/production -run 'TestCertificateRotationOperation' -count=1`
  通过。
- A668 冻结 direct certificate rotation validator 的 credential 输入：
  A667 覆盖了 operation runner，但手工/恢复执行 `validate-certificate-rotation.sh` 时，
  begin/overlap/complete 仍会直接从调用方提供的 OLD/NEW/OVERLAP credential 路径执行
  health 与 fingerprint 读取；本地文件若在 fingerprint/health/receipt 发布窗口被替换，
  durable state 或完成 receipt 可能绑定不同输入。现在 validator 每次 invocation 会把所需
  credential 捕获到私有临时目录，要求源路径和副本的 whole-file SHA 在捕获窗口内一致，
  后续 health、fingerprint 与 receipt 复用全部只读冻结副本；捕获漂移时 fail closed。回归：
  `go test ./hack/production -run 'TestValidateCertificateRotation|TestCertificateRotationOperation' -count=1`
  通过。
- A669 收紧 operation runner 的 receipt digest 稳定性：
  Backup、Destroy、RestoreCutover、PostRestoreAudit、CertificateRotation 与
  BackupDeletion runner 在提交 succeed 前都会先 validate receipt、计算 SHA，再复检
  receipt 语义；旧逻辑没有要求复检后的文件 bytes 仍等于即将提交的 digest，若本地 receipt
  在 hash 后被替换成另一个语义仍合法但 bytes 不同的 JSON，operation 状态可能记录 stale
  receipt SHA。现在相关 `validated_*_receipt_digest` helper 都执行
  validate → hash → validate → hash → compare，任何 digest 窗口漂移都会 fail closed；
  BackupDeletion 的 pre inventory、deletion、post inventory 子 receipt 也使用同样稳定性
  fence。回归：
  `go test ./hack/production -run 'TestBackupOperation|TestDestroyOperation|TestRestoreCutoverOperation|TestPostRestoreAuditOperation|TestCertificateRotationOperation|TestBackupDeletionOperation' -count=1`
  通过。
- A670 冻结 boundary cleanup 的 destroy receipt 授权输入：
  `cleanup-instance-boundaries.sh` 会用 destroy receipt 授权删除实例数据命名空间与外部
  credential secret，旧逻辑在每次 action 中反复从调用方路径读取
  `DESTROY_RECEIPT_INPUT`；若 destroy receipt 在 validate/state/header/receipt 窗口被替换，
  cleanup state 可能绑定不同授权 bytes，甚至在 delete 后才因 receipt 漂移导致 complete
  无法收敛。现在脚本入口会把 destroy receipt 捕获到私有临时副本，要求源路径和副本的
  whole-file SHA 在捕获窗口内一致，后续授权校验、state header 与 completion receipt 全部
  使用冻结副本；捕获漂移时 fail closed。回归：
  `go test ./hack/production -run 'TestBoundaryCleanup' -count=1`
  通过。
- A671 冻结 BackupDeletion runner 的源 evidence 输入：
  `run-backup-deletion-operation.sh` 旧逻辑只在参数解析后对 source backup receipt、pre
  manifest 与 post manifest 各做一次 SHA 对比，随后 inventory/delete gate 继续读取调用方
  路径；若这些本地 evidence 在参数 digest 校验后被替换，删除 workflow 可能消费未被参数
  SHA 绑定的对象版本或 inventory 集合。现在 runner 会把三份 evidence 按参数 SHA 捕获到
  私有临时目录，要求源路径与副本在捕获窗口内都等于参数 digest，后续 source receipt parse、
  manifest gate 和 object workflow 全部使用冻结副本；捕获漂移 fail closed，捕获后的原始
  source receipt 漂移不再影响 workflow。回归：
  `go test ./hack/production -run 'TestBackupDeletionOperation' -count=1`
  通过。
- A672 冻结 cold restore executor 的 restore manifest 输入：
  A656/A660 已让 cold restore executor 从私有副本消费 snapshot receipt 和 canonical
  applied manifest，但 `RESTORE_MANIFEST` 在最初 `jq -cS` 前仍直接来自调用方路径；若本地
  manifest 在 hash/copy 窗口被替换，executor 可能把 source path 的可追溯性与实际 apply
  内容混淆。现在 executor 会先把 restore manifest 捕获到私有 0600 副本，并要求源路径
  digest 在捕获前后与副本一致；后续解析、canonical renderer 比对和 `kubectl create`
  全部只读冻结副本。捕获窗口漂移会在 target access 前 fail closed，捕获后的原路径漂移
  仍不影响已验证 manifest 的 apply。回归：
  `go test ./hack/production -run 'TestColdRestoreExecute' -count=1`
  通过。
- A673 冻结 cold snapshot executor 的 semantic witness 输入：
  A657 已在 `logical-status` 前后复检 witness 文件 SHA，但 status 仍直接读取调用方
  `SEMANTIC_WITNESS_FILE` 路径；本地文件若在 status 读取窗口中被短暂替换再恢复，snapshot
  receipt 可能把 witness status 与另一组 bytes 的文件 digest 混用。现在 executor 会把
  semantic witness 捕获到私有 0600 副本，要求源路径与副本在捕获前后一致，`logical-status`
  和最终 receipt 发布前复检都只读冻结副本；捕获窗口漂移会在任何存储变更前失败，捕获后的
  原路径漂移不再影响已验证 witness。回归：
  `go test ./hack/production -run 'TestColdSnapshotExecute' -count=1`
  通过。
- A674 冻结 Backup operation runner 的参数输入：
  `run-backup-operation.sh` 旧逻辑在 claim 返回 `parameters_sha256` 后只对
  `PARAMETERS_INPUT` 做一次 digest 比对，随后继续从原始路径解析 endpoint、artifact、
  Object Lock 目标和 receipt 输出路径；本地参数文件若在 digest 读取后被替换，runner 可能
  消费未被 operation claim 绑定的参数。现在 runner 会把参数文件复制到私有临时目录，要求
  原路径与副本在捕获窗口内都等于 claimed SHA，后续字段解析只读冻结副本；捕获漂移会走
  原有 retry 路径，不进入 export/object workflow。回归：
  `go test ./hack/production -run 'TestBackupOperation' -count=1`
  通过。
- A675 冻结 Destroy operation runner 的参数输入：
  `run-destroy-operation.sh` 旧逻辑在 claimed parameter digest 通过后继续从原始
  `PARAMETERS_INPUT` 解析 backup path、confirmation token、namespace 与 state/receipt 路径；
  若参数文件在 digest 后被替换，runner 可能对未被 claim 绑定的实例或 artifact 执行 destroy
  phase。现在 runner 使用私有目录捕获参数副本，要求源路径和副本在捕获窗口内都等于
  claimed SHA，后续 JSON 解析只读冻结副本；原有 backup artifact 副本也迁入同一私有目录。
  参数捕获漂移走 retry，且不会进入 prepare/quiesce/destroy/complete phase。回归：
  `go test ./hack/production -run 'TestDestroyOperation' -count=1`
  通过。
- A676 冻结 RestoreCutover operation runner 的参数输入：
  `run-restore-cutover-operation.sh` 旧逻辑在 claim 参数 SHA 校验后继续从原始路径解析
  restore receipt、backup artifact、service selector 与 cutover state/receipt 输出位置；若
  参数文件在 digest 后被替换，runner 可能对未绑定的流量目标执行 prepare/cutover。现在
  runner 将参数捕获到私有 0600 副本，要求源路径和副本在捕获窗口内都等于 claimed SHA，
  后续 phase env 构造只读冻结副本；捕获漂移会 retry，且不会进入 prepare。回归：
  `go test ./hack/production -run 'TestRestoreCutoverOperation' -count=1`
  通过。
- A677 冻结 PostRestoreAudit operation runner 的参数输入：
  `run-post-restore-audit-operation.sh` 旧逻辑在 claimed SHA 比对后继续从原始
  `PARAMETERS_INPUT` 解析 cutover evidence、service identity、probe endpoint 和 audit
  receipt 输出；若参数在 digest 后被替换，runner 可能审核未被 operation claim 绑定的
  cutover 链。现在 runner 捕获参数到私有 0600 副本，要求源路径与副本在捕获窗口内都等于
  claimed SHA，后续 audit env 构造只读冻结副本；捕获漂移走 retry 且不写 audit receipt。
  回归：`go test ./hack/production -run 'TestPostRestoreAuditOperation' -count=1`
  通过。
- A678 冻结 BackupDeletion operation runner 的参数输入：
  A671 已冻结 source receipt/pre manifest/post manifest 三份 evidence，但
  `run-backup-deletion-operation.sh` 仍在 claimed parameter digest 通过后从原始
  `PARAMETERS_INPUT` 解析这些 evidence 路径和输出 receipt 路径；若参数文件在 digest 后被
  替换，runner 可能删除未被 claim 绑定的对象版本。现在 runner 复用 evidence 私有目录捕获
  参数副本，要求源路径与副本在捕获窗口内都等于 claimed SHA，后续参数解析和 evidence
  capture 都只由冻结参数驱动；捕获漂移会 retry，且不会进入 object workflow。回归：
  `go test ./hack/production -run 'TestBackupDeletionOperation' -count=1`
  通过。
- A679 冻结 CertificateRotation operation runner 的参数输入：
  A667 已冻结 credential 文件并重写 step 参数，但 runner 在 claimed parameter digest 通过后
  仍从原始 `PARAMETERS_INPUT` 解析 credential 路径、endpoint 与输出 receipt；若参数文件在
  digest 后被替换，后续 credential 捕获和 gate/hook 参数可能来自未被 claim 绑定的 JSON。
  现在 runner 先把参数捕获到私有 0600 副本，要求源路径与副本在捕获窗口内都等于 claimed
  SHA，再从冻结副本解析 credential 并生成 step 专用参数副本；捕获漂移走 retry，且不进入
  begin gate。回归：
  `go test ./hack/production -run 'TestCertificateRotationOperation' -count=1`
  通过。
- A680 冻结 PostRestoreAudit 直接执行脚本的 cutover evidence 输入：
  A677 已冻结 operation runner 的参数 JSON，但 `audit-restored-instance.sh` 仍在原始
  `CUTOVER_STATE_INPUT`/`CUTOVER_RECEIPT_INPUT` 路径上做 schema、JQ 校验和最终 digest
  复核；若输入路径在初始校验后漂移，审计过程可能读取未被捕获窗口绑定的 cutover
  evidence。现在脚本在任何解析前把 cutover state 与 cutover receipt 捕获到私有 0600
  副本，要求源文件在捕获前后与副本 digest 一致，并只把冻结路径传给后续 probe/JQ；
  捕获漂移会 fail closed 且不发布 audit receipt。回归：
  `go test ./hack/production -run 'TestPostRestoreAudit' -count=1`
  通过。
- A681 冻结 RestoreTrafficCutover 直接执行脚本的外部输入：
  A676 已冻结 cutover operation runner 参数，但 `switch-restore-traffic.sh` 的 prepare/
  verify/complete 阶段仍从原始 `RESTORE_RECEIPT_INPUT` 和 `BACKUP_INPUT` 读取 restore
  verification receipt 与 logical backup；若这些路径在初始校验后漂移，cutover 可能基于
  未被捕获窗口绑定的 restore evidence 或验证数据继续推进。现在脚本按 action 需要捕获
  restore receipt 与 backup 到私有 0600 副本，要求源文件在捕获前后与副本 digest 一致，
  并只把冻结 backup path 传给 logical verify；捕获漂移 fail closed 且不写 phase marker。
  回归：`go test ./hack/production -run 'TestRestoreTrafficCutover' -count=1`
  通过。
- A682 冻结 DestroyInstance 直接执行脚本的备份输入：
  A675 已让 destroy operation runner 复制 claimed backup artifact，但 `destroy-instance.sh`
  的直接 prepare 入口仍在原始 `BACKUP_INPUT` 路径上做 file digest、logical-status 和最终
  digest 复核；若备份路径在初始 digest 后漂移，prepare 可能基于未被捕获窗口绑定的
  backup evidence 记录 destroy state。现在 direct script 在 prepare 开始先把 backup
  捕获到私有 0600 副本，要求源文件在捕获前后与副本 digest 一致，并只把冻结路径传给
  logical-status；捕获漂移 fail closed 且不写 destroy state。回归：
  `go test ./hack/production -run 'TestDestroyInstance' -count=1`
  通过。
- A683 冻结 objectstore upload 的本地 artifact 输入：
  `objectstore.Upload` 旧逻辑先 inspect 原始 `INPUT`，再 stat/open 原路径，并用另一次
  path hash 生成 metadata/checksum；若本地 artifact 在 inspect、hash、PutObject 之间
  漂移，可能不会发布 receipt，但已经把不匹配对象写入带 Object Lock 的远端版本。现在
  Upload 复用 `backupfile.OpenVerified` 的私有副本：先复制并验证 local artifact，再只从
  frozen path 计算 size、file SHA、S3 checksum 和 PutObject body；原始输入在 PutObject
  前被替换也不会污染远端对象。回归：
  `(cd hack/backup/objectstore && go test ./internal/objectstore -run 'TestUpload' -count=1)`
  通过。
- A684 冻结 objectstore audit archive 的远端验证输入：
  A522 已让 `ArchiveAudit` 上传 `operationaudit.InspectBytes` 返回的 frozen bytes，但
  `verifyAuditRemote` 在远端下载后又重新读取原始 `request.Input` 做 byte compare；若原始
  audit artifact 在 PutObject 后漂移，可能远端对象正确但 receipt 不发布，留下难以复用的
  Object Lock 版本。现在远端验证只比较最初捕获的 frozen audit bytes，原始输入在上传前后
  被替换也不会影响已验证远端对象和 receipt。回归：
  `(cd hack/backup/objectstore && go test ./internal/objectstore -run 'TestArchiveAudit' -count=1)`
  通过。
- A685 绑定 RestoreCutover operation 的外部 evidence SHA：
  A676 已冻结 RestoreCutover runner 的参数 JSON，A681 已让直接执行脚本冻结
  restore receipt 与 backup 输入，但 operation 参数本身仍只携带路径，未声明这些输入的
  whole-file digest；若 claim 参数已冻结后本地 evidence 路径被替换为另一份格式合法内容，
  runner 只能让子状态机基于替换后的 bytes 推进。现在
  `run-restore-cutover-operation.sh` 要求参数包含 `restore_receipt_sha256` 与
  `backup_file_sha256`，先按参数 SHA 捕获两份 evidence 到私有 0600 副本，并只把冻结路径
  传给 cutover 子状态机；源路径 digest 不匹配或捕获窗口漂移都会 retry，且不会进入
  prepare。回归：
  `go test ./hack/production -run 'TestRestoreCutoverOperation' -count=1`
  通过。
- A686 绑定 PostRestoreAudit operation 的 cutover evidence SHA：
  A677 已冻结 PostRestoreAudit runner 的参数 JSON，A680 已让直接执行脚本冻结
  `CUTOVER_STATE_INPUT` 与 `CUTOVER_RECEIPT_INPUT`，但 operation 参数仍未声明这两份
  cutover evidence 的 whole-file digest；claim 后若本地路径被替换，audit 子状态机可能
  基于未被 operation 参数绑定的 cutover 链生成审计 receipt。现在
  `run-post-restore-audit-operation.sh` 要求参数包含 `cutover_state_sha256` 与
  `cutover_receipt_sha256`，先按参数 SHA 捕获 state/receipt 到私有 0600 副本，并只把冻结
  路径传给 audit 子状态机和最终 receipt 校验；源路径 digest 不匹配或捕获窗口漂移都会
  retry，且不会启动 audit。回归：
  `go test ./hack/production -run 'TestPostRestoreAuditOperation' -count=1`
  通过。
- A687 冻结手工 operation audit archive 的 release 输入：
  `archive-operation-audit.sh` 会先把终态 Operation capture 到 `ARTIFACT_OUTPUT`，再调用
  Object Lock archive，最后用 `operation-audit --action release` 移除 audit finalizer；旧脚本
  archive 与 release 都继续读取用户传入的 artifact/receipt 输出路径。若这些路径在 capture、
  Object Lock receipt 生成或 release 前后漂移，可能让 release 消费与实际 archive 子流程
  不同的本地 bytes，造成已上传版本无法复用或 release 失败。现在脚本在 capture 后把
  artifact 捕获到私有 0600 副本，archive 和 release 都只读该副本；Object Lock receipt
  生成后同样捕获到私有副本再传给 release。捕获窗口漂移 fail closed，且不会进入后续阶段。
  回归：`go test ./hack/production -run 'TestArchiveOperationAudit' -count=1` 通过。
- A688 冻结 Backup operation runner 的导出 artifact：
  A683 已让 Object Lock upload 在模块内部冻结 `INPUT`，但 `run-backup-operation.sh` 仍在
  logical export、status、upload 和最终 receipt 校验之间反复读取用户参数里的
  `artifact_output` 路径；若该路径在 upload 前后漂移，远端对象可能已经正确写入但 runner
  后续校验改读另一份本地 artifact，导致 operation 无法收敛或 receipt 绑定混乱。现在 runner
  在 export/复用既有 artifact 后立即把 artifact 捕获到私有 0600 副本，status、upload 和
  succeed 前 receipt 校验全部只读该副本；原始 artifact path 后续漂移不会影响已冻结
  workflow，捕获窗口漂移会 retry 且不会进入 upload。回归：
  `go test ./hack/production -run 'TestBackupOperation' -count=1` 通过。
- A689 冻结 BackupDeletion operation runner 的生成 receipt：
  A671/A678 已冻结 source receipt 与 inventory manifest 输入，但
  `run-backup-deletion-operation.sh` 在 Object Lock inventory/delete/post-inventory workflow
  生成三份子 receipt 后，仍从用户参数里的输出路径读取并计算最终 operation receipt。若这些
  输出路径在 workflow 完成后漂移，删除已经发生但 runner 可能用另一份本地 receipt 校验或
  合成最终 receipt。现在 workflow 完成后立即把 pre inventory、deletion、post inventory
  三份 receipt 捕获到私有 0600 副本，后续 schema 校验、SHA 计算和 operation receipt 合成
  全部只读副本；捕获窗口漂移 fail closed。回归：
  `go test ./hack/production -run 'TestBackupDeletionOperation' -count=1` 通过。
- A690 将 TiKV `StaleCommand` 提交错误归入不确定结果：
  对照 `github.com/tikv/client-go/v2@v2.0.7/internal/locate/region_request.go`
  中对 stale command 的注释，若请求进入提交阶段后发到旧 leader，客户端不能证明
  写是否已经持久化。此前 `pkg/storage/tikv/batch.go` 只把 deadline/cancel、
  server timeout、unknown、`ErrResultUndetermined` 和 `TxnLockNotFound` 映射为
  `storage.ErrUncertainResult`；若 `ErrTiKVStaleCommand` 逃逸到 `Commit`，上层会把它当
  确定失败，绕过 backend 的异步不确定写修复队列。现在将 wrapped/unwrapped
  `tikverr.ErrTiKVStaleCommand` 纳入 `isUncertainCommitError`，由现有 async retry
  重新解析最终状态。回归：`go test ./pkg/storage/tikv -run TestUncertainCommitError -count=1`
  通过。
- A691 收紧 NetworkPolicy release gate 的 namespace 输入：
  `validate-network-policy.sh` 旧校验把 namespace 与 Pod/host 共用同一个允许 `.` 的
  Kubernetes 名称正则；但 namespace 只能是 DNS label，非法 `client.ns` 会进入后续
  `kubectl get namespace`/`run pod` 阶段才失败，放大生产发布门禁的误配窗口。现在
  `CLIENT_NAMESPACE`、`MONITORING_NAMESPACE`、`DENIED_NAMESPACE`、`KUBEBRAIN_NAMESPACE`
  和 `TIDB_NAMESPACE` 都在首个 Kubernetes 调用前按小写 63 字符 DNS label 拒绝；
  `KUBEBRAIN_POD` 仍按 DNS subdomain 逐段校验，保留合法 Pod 名兼容。回归：
  `go test ./hack/production -run TestValidateNetworkPolicy -count=1` 通过。
- A692 收紧 operation shell 入口的 namespace 输入：
  A691 后继续审计直接 executor 与 runner，发现销毁、边界清理、恢复切流、恢复后审计、
  证书轮换、operation audit 归档以及六类 operation runner 仍可能把带 `.` 的 namespace
  传给 `kubectl` 或 `operationctl`，直到 Kubernetes API 报错才失败。现在这些入口的
  `KUBEBRAIN_NAMESPACE`、`TIDB_NAMESPACE`、`CREDENTIAL_NAMESPACE`、`SERVICE_NAMESPACE`
  和 `OPERATION_NAMESPACE` 都在首次外部命令前按 DNS label 拒绝，runner 对 claim response
  中固化的 namespace 也会重新校验。集中回归使用 fake `kubectl`/`operationctl`/`go`，
  证明非法 namespace 不会触发任何外部调用：
  `go test ./hack/production -run TestProductionShellEntrypointsRejectInvalidNamespacesBeforeExternalCalls -count=1`
  通过。
- A693 收紧 Go 管理面 CLI 的 namespace 输入：
  A692 关闭 shell wrapper 后，直接执行的 Go 二进制仍可能接受非法 `--namespace` 或
  `--namespace-inventory-namespace`，随后在 kubeconfig/client 初始化或 Kubernetes API 调用中
  才失败。现在 `namespaceinventory.ValidateOne` 成为单 namespace DNS label 校验入口；
  operationctl、Operation API、parameter broker、operation archiver、operation audit、
  backup scheduler 和 UID delete 都在加载 kubeconfig 前 fail fast。backup scheduler 也把
  静态 `--namespace`/`--namespaces` allowlist 解析前移到 client 初始化前。命令级回归用
  `go run .` 证明非法 namespace 不会进入 kubeconfig 路径：
  `go test ./hack/production/internal/namespaceinventory ./hack/production/cmd/backup-scheduler ./hack/production/cmd/operationctl ./hack/production/cmd/uid-delete ./hack/production/cmd/operation-api ./hack/production/cmd/operation-parameter-broker ./hack/production/cmd/operation-archiver ./hack/production/cmd/operation-audit -run 'TestValidateOneRequiresDNSLabelNamespace|TestMainRejectsInvalid|TestParseNamespacesRejectsUnsafeAllowlist' -count=1`
  通过。
- A694 下沉 namespace inventory reader 的 namespace 校验：
  A693 在 CLI 层前置校验 `--namespace-inventory-namespace`，但 `namespaceinventory.Load`
  自身仍会直接向传入 namespace 发起 ConfigMap GET；未来库调用方若绕过 CLI，非法
  `control.ns` 会再次等到 Kubernetes API 层失败。现在 Load 在任何 API action 前调用
  `ValidateOne`，并把错误标记为 `inventory namespace`。fake dynamic client 回归确认
  非法 namespace 返回错误且 `client.Actions()` 为空：
  `go test ./hack/production/internal/namespaceinventory -count=1` 通过。
- A695 下沉 namespace inventory ConfigMap identity 校验：
  A694 已前置校验承载 namespace，但 `namespaceinventory.Load` 仍直接使用调用方传入的
  ConfigMap 名称和 data key。非法 `inventory/name` 或 `namespaces/json` 会进入
  Kubernetes API GET、缺 key 阶段或 client 初始化后才失败。现在 `ValidateSource` 统一
  校验承载 namespace、ConfigMap 名称和 data key；Load 在 API action 前调用它，
  operationctl、backup scheduler 和 operation archiver 在 kubeconfig/client 初始化前调用它，
  operation archiver controller 构造期也会拒绝非法 source。错误分别标记为
  `inventory configmap` 与 `inventory data key`；fake dynamic client 回归确认两类非法
  identity 都不会产生 Kubernetes API action。回归：
  `go test ./hack/production/internal/namespaceinventory ./hack/production/internal/operationarchiver -count=1`
  和
  `go test ./hack/production/cmd/backup-scheduler ./hack/production/cmd/operationctl ./hack/production/cmd/operation-archiver -run 'TestMainRejectsInvalid|TestParseNamespacesRejectsUnsafeAllowlist' -count=1`
  通过。
- A696 下沉 operation queue namespace 校验：
  A693/A695 关闭了 CLI 与 inventory reader 的 namespace 输入面，但
  `operationqueue.New(client, namespace)` 仍是无错误构造函数；未来库调用方若直接传入
  `tenant.a`，Submit/Claim/Get/Parameters/Approve/Delete 等公开方法会带非法 namespace
  进入 Kubernetes API action。现在 Queue 的所有会触发 API 的公开入口都会先校验 queue
  namespace 为 DNS label，`ClaimAcrossNamespaces` 也会先校验完整 allowlist，再检查任何
  namespace 的 last-started watermark。fake dynamic client 回归覆盖 Submit、Claim、
  Requeue、Heartbeat、Finish、Get、Parameters、ParametersForWorker、Approve、Delete 和
  cross-namespace claim，证明非法 namespace 返回错误且 `client.Actions()` 为空：
  `go test ./hack/production/internal/operationqueue -count=1` 通过。
- A697 下沉 operation queue operation name 校验：
  Submit 已校验 operation resource name，但 Requeue、Heartbeat、Finish、Get、Parameters、
  ParametersForWorker、Approve 和 Delete 会直接把调用方传入的 name 交给 dynamic client；
  未来库调用方传入 `backup/1` 时会进入 Kubernetes API path 层才失败。现在 operation
  queue 复用统一 `validateOperationName`，所有按 name 访问 Operation 的公开入口都会在
  API action 前拒绝非 DNS-subdomain 名称；Submit 继续把该错误归类为 `ErrInvalidSpec`。
  fake dynamic client 回归覆盖全部 name-based 方法，证明非法 name 返回错误且
  `client.Actions()` 为空：
  `go test ./hack/production/internal/operationqueue -count=1` 通过。
- A698 前置 operation queue worker fencing identity 校验：
  Requeue、Heartbeat、Finish 和 ParametersForWorker 虽会在对象读取后通过
  `requireWorker`/状态比较拒绝错误 owner 或 attempt，但空 owner、非正 attempt、带控制字符的
  owner，以及 ParametersForWorker 的不支持 operation type 仍可能先触发 Operation GET。
  现在 `validateWorkerIdentity` 在这些公开入口的 API action 前统一校验 owner 非空、audit
  text 规则和 positive attempt；ParametersForWorker 还会先拒绝不支持的 operation type。
  fake dynamic client 回归覆盖 requeue/heartbeat/finish/parameters-for-worker 的非法 worker
  identity，证明返回错误且 `client.Actions()` 为空：
  `go test ./hack/production/internal/operationqueue -count=1` 通过。
- A699 前置 operation queue UID delete precondition 校验：
  UID delete CLI 已要求 `--uid` 非空，但 `operationqueue.Delete` 作为库入口仍会把空
  `types.UID` 放入 delete precondition 并发给 Kubernetes API；未来库调用方绕过 CLI 时，
  空 UID fencing 证据要等 API 层才失败。现在 Delete 在任何 API action 前要求非空 UID
  precondition；fake dynamic client 回归确认空 UID 返回错误且 `client.Actions()` 为空：
  `go test ./hack/production/internal/operationqueue -count=1` 通过。
- A700 前置 operation queue claim type filter 校验：
  Claim 与 ClaimAcrossNamespaces 的 `operationType` 过滤器允许空字符串表示任意类型，但非空
  不支持值此前会先触发 Operation List/last-started inspection，随后自然找不到工作。现在
  `validateOperationTypeFilter` 在任何 queue listing 前拒绝不支持的非空 operation type，
  `lastStarted` 也复用同一规则；fake dynamic client 回归确认单 namespace claim 和
  cross-namespace claim 的 `Unsupported` filter 都返回错误且 `client.Actions()` 为空：
  `go test ./hack/production/internal/operationqueue -count=1` 通过。
- A701 前置 operation parameter Secret 引用校验：
  Submit 会校验 `parametersSecretRef.name`、`key` 和 `parametersSHA256`，但
  `Queue.Parameters`/`ParametersForWorker` 从既有 Operation 对象读取引用后会直接 GET
  Secret；旧 CRD、恢复对象或 fake-client 路径若带入 `Invalid_Secret`、`parameters/json`
  或非 SHA-256 digest，会先触发 Secret API 或读取不可能由 Submit 生成的证据。现在
  `parameters()` 在 Secret GET 前重新校验 Secret 名称为 DNS subdomain、key 符合
  CRD 参数 key 规则、digest 为 lowercase SHA-256。fake dynamic client 回归确认 malformed
  Operation 只产生 Operation GET，不产生 Secret GET：
  `go test ./hack/production/internal/operationqueue -run 'TestQueueRejectsMalformedParameterReferenceBeforeSecretAPI|TestQueueLoadsDigestBoundImmutableParameters|TestQueueOnlyLoadsParametersForCurrentTypeBoundWorker' -count=1`
  通过。
- A702 下沉 operation parameter broker identity namespace 校验：
  CLI 已在 client 初始化前校验 `--namespace`，但 `parameterbroker.NewHandler` 作为库入口
  只要求 namespace 非空；未来调用方直接传入 `ops.ns` 时，Ready 会用非法 namespace
  探测 Operation/Secret API。现在 NewHandler 在构造期要求 identity namespace 为
  Kubernetes DNS label，错误标记为 `invalid parameter broker namespace`。fake client
  回归确认非法 namespace 构造失败且 dynamic/token clients 都没有 API action：
  `go test ./hack/production/internal/parameterbroker ./hack/production/cmd/operation-parameter-broker -count=1`
  通过。
- A703 下沉 backup scheduler 静态 namespace allowlist 校验：
  A693 已让 backup scheduler CLI 在 client 初始化前校验 `--namespace`/`--namespaces`，
  A264 已让动态 inventory 在 policy list 前 fail closed；但库入口
  `backupscheduler.NewForNamespaces` 仍会原样保存静态 allowlist，未来调用方直接传入
  `tenant.a`、空列表或重复 namespace 时，会进入 policy list 或产生静默空轮次。现在
  scheduler 在每轮 reconcile 的首个 Kubernetes API action 前复用
  `namespaceinventory.Validate`，静态 allowlist 必须非空、无重复且全部为 Kubernetes DNS
  label。fake dynamic client 回归确认非法 allowlist 返回错误且没有任何 API action：
  `go test ./hack/production/internal/backupscheduler ./hack/production/cmd/backup-scheduler -count=1`
  通过。
- A704 前置 audit finalizer release identity 校验：
  operation-audit CLI 已在 client 初始化前校验 `--namespace`，capture 路径也通过
  operation queue 的 `Get` 继承 operation name 校验；但
  `operationauditrelease.ReleaseWithExpectedReceipt` 作为库入口在读取 artifact/receipt 后才
  用 namespace/name 发起 Operation GET。未来调用方直接传入 `ops.ns` 或 `backup/1` 时，
  错误会等到证据文件解析或 Kubernetes API 阶段才暴露。现在 release 在本地证据读取前校验
  namespace 为 Kubernetes DNS label、Operation name 为 DNS subdomain。fake dynamic client
  回归使用不存在的证据路径，确认非法 identity 返回错误且没有任何 API action：
  `go test ./hack/production/internal/operationauditrelease ./hack/production/cmd/operation-audit -count=1`
  通过。
- A705 前置 UID-fenced delete request identity 校验：
  A187/A693 让销毁流程使用 `uid-delete` 并在 CLI 层校验 namespace，但 `deleteWithUID`
  库函数仍会把调用方传入的 resource、name 和空 UID 直接交给 dynamic client；非法
  `pods/status`、`pod/a` 或空 UID 要等 REST path/body 阶段才失败。现在主函数和
  `deleteWithUID` 复用 `validateDeleteRequest`：apiVersion 必须可解析且 group/version 符合
  Kubernetes DNS 形状，resource 必须是 DNS label，namespace 若存在必须是 DNS label，
  resource name 必须是 DNS subdomain，UID precondition 必须非空。HTTP fake server 回归确认
  非法 resource/namespace/name/UID 都返回本地错误且不会发出 DELETE：
  `go test ./hack/production/cmd/uid-delete -count=1`
  通过。
- A706 前置 BackupPolicy metadata identity 校验：
  A573 已让 backup scheduler 在读取参数模板前校验 policy spec，但 metadata.name/uid 仍依赖
  apiserver 正常填充。未来 fake-client、恢复对象或旧对象若缺 UID，scheduler 会创建 owner
  fence 为空的 immutable 参数 Secret；若 name 不符合 Kubernetes resource name，又会参与
  Operation ID 与 Secret 名称生成。现在 `reconcilePolicy` 在读取模板 Secret 前要求
  BackupPolicy name 为 DNS subdomain、UID 非空。fake dynamic client 回归确认 malformed
  metadata 的 reconcile 只产生 policy list，不读取模板 Secret、不创建参数 Secret 或
  Operation：
  `go test ./hack/production/internal/backupscheduler ./hack/production/cmd/backup-scheduler -count=1`
  通过。
- A707 前置 parameter broker query identity 校验：
  parameter broker 已严格解析 Bearer header，但旧请求路径会先执行 TokenReview，再校验
  `namespace/name/owner/attempt` query。malformed `ops.ns`、`audit/1` 或非正 attempt 虽最终
  返回 400，却会消耗认证 API，且 operation name 形状要等 operation queue 读取前才兜底。
  现在 handler 在 TokenReview 前解析并校验 query identity：四个必需参数必须各恰好出现一次，
  namespace 为 DNS label，name 为 DNS subdomain，owner 非空，attempt 为正整数。fake
  Kubernetes/dynamic client 回归确认 malformed query 返回 400 且 TokenReview、Operation、
  Secret API action 均为空：
  `go test ./hack/production/internal/parameterbroker ./hack/production/cmd/operation-parameter-broker -count=1`
  通过。
- A708 前置 backup scheduler requester 校验：
  scheduler 的 `WithRequester` 值会写入 immutable Operation spec 与终态审计。旧路径只在
  `operationqueue.Submit` 阶段校验 requestedBy，库调用方若传入控制字符或非 UTF-8 requester，
  scheduler 可能已经渲染参数、创建 immutable 参数 Secret，然后才被 queue 拒绝。现在
  operation queue 导出同一 requestedBy audit text 校验，scheduler 每轮 reconcile 在读取
  namespace inventory 或 list policy 前先校验 requester。fake dynamic client 回归确认非法
  requester 返回错误且没有任何 Kubernetes API action：
  `go test ./hack/production/internal/backupscheduler ./hack/production/internal/operationqueue ./hack/production/cmd/backup-scheduler -count=1`
  通过。
- A709 前置 parameter broker worker owner 校验：
  A707 已把 parameter broker 的 namespace/name/attempt query 校验前移到 TokenReview 前，但
  owner 仍只检查非空；带控制字符、非 UTF-8 或超长 owner 的请求仍会消耗 TokenReview，随后才
  由 operation queue 的 ParametersForWorker 拒绝。现在 operation queue 导出统一 worker
  identity 校验，broker query 解析在 TokenReview 前同时校验 owner audit text 与 positive
  attempt。fake Kubernetes/dynamic client 回归确认 malformed owner 返回 400 且没有
  TokenReview、Operation 或 Secret API action：
  `go test ./hack/production/internal/parameterbroker ./hack/production/internal/operationqueue ./hack/production/cmd/operation-parameter-broker -count=1`
  通过。
- A710 前置 operation API submit body 校验：
  外部 operation API 的 `POST /v1/operations` 原先先执行 OIDC/JWKS 认证，再拒绝错误
  `Content-Type`、unknown field、trailing JSON 或超限 body；明显畸形提交仍会消耗
  认证依赖。现在 submit handler 在认证前完成 JSON media type、64 KiB body 上限、
  unknown-field 和单对象 trailing 校验；回归使用计数 authenticator 确认 malformed submit
  返回 400/415 且没有调用 authenticator 或 Operation store：
  `go test ./hack/production/internal/operationapi ./hack/production/cmd/operation-api -count=1`
  通过。
- A711 前置 operation API operation name 校验：
  A697 已让 operation queue 的 name-based 库入口在 Kubernetes API 前拒绝非法 Operation
  name，但外部 operation API 的 GET path name 和 submit body name 仍会先消耗 OIDC/JWKS，
  再由 queue 层兜底。现在 operation queue 导出统一 operation name 校验，API handler 在
  认证前拒绝非法 `POST` body name 与 `GET /v1/operations/{name}` path name。回归确认
  malformed name 返回 400 且没有调用 authenticator 或 Operation store：
  `go test ./hack/production/internal/operationapi ./hack/production/internal/operationqueue ./hack/production/cmd/operation-api -count=1`
  通过。
- A712 前置 readiness method 校验：
  operation API 与 parameter broker 的 `/readyz` 是生产 Service 接流门禁，但 main-level
  handler 原先不区分 HTTP method；非 GET 请求也会执行 TLS 证书检查和 Kubernetes/OIDC
  依赖探测。现在两个 `/readyz` 都只接受 GET，其他 method 在任何证书或依赖检查前返回
  405 并设置 `Allow: GET`。回归使用计数 certificate/dependency fake 确认 POST `/readyz`
  不触发探测：
  `go test ./hack/production/cmd/operation-api ./hack/production/cmd/operation-parameter-broker -count=1`
  通过。
- A713 扩展 Lock/Election API surface 静态门禁：
  KubeBrain 已把 dedicated Lock/Election gRPC 与 JSON gateway 作为兼容核心服务暴露，
  但 `TestEtcdAPISurfaceIsExplicit` 原先只枚举 KV、Watch、Lease、Cluster、
  Maintenance 和 Auth。若未来 etcd API 依赖新增 Lock/Election RPC，运行时可能只因
  `Unimplemented*Server` 嵌入而出现 accidental surface。现在静态门禁同时枚举
  `v3lockpb.Lock_ServiceDesc` 与 `v3electionpb.Election_ServiceDesc`，并要求
  `lockServer`/`electionServer` 为每个公开方法提供显式 wrapper；Unimplemented
  status 分类也纳入这两个 owner，防止已承诺支持的 concurrency RPC 被静默降级。
  `go test ./pkg/server/etcd -run TestEtcdAPISurface -count=1` 通过。
- A714 固定 generated HTTP gateway service surface：
  A713 锁住了 gRPC service descriptor 与 production receiver 的关系，但 endpoint
  的 generated JSON gateway 注册列表仍是 `newGRPCGatewayMux` 内部匿名函数 slice，
  缺少可直接审查的 surface 门禁。现在 gateway 注册表提升为带 service descriptor
  name 的 `grpcGatewayRegistrations`，运行时仍按原顺序注册 KV、Watch、Lease、
  Cluster、Maintenance、Auth、Lock、Election；注册失败错误会带具体 service name。
  新增 `TestGRPCGatewaySurfaceIsExplicit` 固定这 8 个公开 HTTP JSON service，
  防止未来升级或重构时漏注册已承诺的 Lock/Election route，或无审查地扩大
  `/v3/*` surface。`go test ./pkg/endpoint -run TestGRPCGatewaySurfaceIsExplicit -count=1`
  通过。
- A715 补齐 Lock/Election protobuf 字段升级门禁：
  A431/A432 已固定 core KV/Watch/Lease/Txn 请求和响应字段，但 dedicated
  Lock/Election API 的 request、response 与 nested `LeaderKey` 仍未纳入字段
  descriptor 审计。未来 etcd 升级若给 Lock、Unlock、Campaign、Proclaim、
  Leader/Observe 或 Resign 增删、改名、重编号字段，KubeBrain 需要同步审计
  concurrency wrapper、generated JSON gateway、auth keyspace 检查和 upstream
  `Unknown` 错误归一化，而不是让新字段静默透传或被 discard。现在新增
  `TestConcurrencyProtoFieldCoverage`，固定 13 个 concurrency protobuf message 的字段名与
  wire number。`go test ./pkg/server/etcd -run TestConcurrencyProtoFieldCoverage -count=1`
  通过。
- A716 精确固定 dedicated concurrency method 分类：
  dedicated Lock/Election 需要保留 upstream 当前的 auth/error 外观，即匿名、
  permission denied 和 invalid token 等错误在该 service 边界归一化为 gRPC
  `Unknown`，而不是走 core etcd 的 `ErrGRPC*` 映射。旧分类使用
  `/v3lockpb.Lock/` 与 `/v3electionpb.Election/` prefix，测试也只抽样覆盖
  Lock、Unlock、Campaign 和 Observe，未显式固定 Proclaim、Leader、Resign，
  且同前缀的虚构方法也会被误判为 dedicated concurrency。现在运行时从
  `v3lockpb.Lock_ServiceDesc` 与 `v3electionpb.Election_ServiceDesc` 构建精确
  full-method allowlist，测试固定当前 7 个 generated full-method constants，并确认同前缀
  unknown method 不命中。`go test ./pkg/server/etcd -run TestDedicatedConcurrencyMethodClassification -count=1`
  通过。

### P2：运维兼容和长期验证

1. `etcdctl` 命令兼容表和平台替代命令的可操作提示已完成；继续随支持版本窗口重跑，
   并把表纳入发布说明。
2. 设计受支持的 transactional TiKV 物理快照/PITR；继续逻辑恢复演练、滚动升级、
   跨可用区故障、磁盘满和长时间 soak。不得用 TiDB BR full/PITR 的成功状态关闭该缺口。
3. Porcupine 已覆盖无故障 Get/Put/CAS、多键 Txn 原子性、lease lifecycle、
   显式 ID revoke/regrant 代际隔离、watch-backed 自然过期，以及可表达不确定写
   结果的 KubeBrain Leader/TiKV/PD 故障历史；批量 lease renewal 隔离模型与小时级
   renewal window 模型已建立；继续在多 store/多 PD 预生产拓扑上做分区和多点故障注入。
   大规模性能测试不能替代正确性证明。

## 提交规则

每个兼容性提交必须同时包含：

1. 对应的 etcd 源码或测试位置；
2. 修复前可复现的差异测试；
3. KubeBrain 单元测试和官方 client/v3 黑盒测试；
4. 涉及存储提交、watch 或 lease 时的真实 TiKV/PD 验证；
5. 本文矩阵状态更新。

不把 etcd 内部 Raft 实现、WAL、bbolt、member reconfiguration 机械移植到
KubeBrain。这些职责已由 TiKV/PD 或 DBaaS 控制面承担；需要对齐的是客户端可
观察的数据语义、错误契约和服务可靠性。
