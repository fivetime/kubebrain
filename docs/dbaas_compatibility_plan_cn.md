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
| KV | Range/Put/DeleteRange | 部分兼容 | P0：事务内排序、过滤、历史读、大范围删除原子性、生产范围上限、非 KEY/NONE/Limit 候选窗口及独立 Put/Range/DeleteRange 差分已补齐；3 PD/3 TiKV 单成员故障持续进度已验证 |
| KV | Txn | 部分兼容 | P0：缺失键 guard、范围 phantom guard、嵌套分支、staged 单 revision 提交及 caller deadline 贯穿后端冲突重试已完成；3 PD/3 TiKV 单成员故障持续进度已验证 |
| KV | Compact | 兼容核心语义 | logical/physical、错误、异步 GC 与请求取消后的后台续扫已对齐；继续长时间故障 soak |
| KV | RangeStream | 兼容核心语义 | etcd 3.7 支持的 CountOnly/Limit/KeysOnly/默认排序已对齐；自定义排序与 revision filter 同 etcd 明确 Unimplemented |
| Watch | create/cancel/progress/history/prevKV/slow-consumer catch-up | 兼容核心语义；后端溢出无缝追赶，控制响应不阻塞接收循环 | P1：继续数天级断线/慢消费者 soak |
| Lease | grant/revoke/keepalive/ttl/list | 兼容核心语义 | meta/attachment 已与用户 revision 隔离并原子提交，Grant durable 后才发布，List 按到期时间稳定排序；继续扩大故障、并发和错误差分矩阵 |
| Auth | 用户、角色、权限、token | 兼容核心语义 | 管理 API、key-range RBAC、token 生命周期、Watch/Lease 持续鉴权及多副本故障转移已验证 |
| Cluster | MemberList | 兼容（需配置） | DBaaS 通过 `--initial-cluster` 注入完整 KubeBrain 服务副本；未配置时仅返回本机与 leader 的降级视图，不应启用 AutoSync |
| Cluster | add/remove/update/promote | 平台替代 | 由 DBaaS 控制面扩缩 KubeBrain、PD、TiKV；RPC 保持明确 Unimplemented |
| Maintenance | Status | 兼容核心语义 | 返回真实服务身份、版本、leader/revision/共享选主 term；bbolt 容量与默认 quota 字段使用兼容 sentinel，真实容量转到实例指标 |
| Maintenance | Snapshot | 平台替代 | 使用 TiKV BR/PITR；控制面提供备份、恢复和导出任务，不伪造 etcd snapshot |
| Maintenance | Defragment | 平台替代 | TiKV GC/compaction 管理，不执行 bbolt 碎片整理 |
| Maintenance | Alarm/DbSize | 平台替代 | 用 PD/TiKV 容量、磁盘、region 和配额告警；etcd 专属字段保持可解释值 |
| Maintenance | Hash/HashKV | 兼容核心语义 | 对指定 revision 的租户 MVCC 实际内容做稳定摘要；用于 KubeBrain 副本一致性校验，数值不与 bbolt 内部编码比较 |
| Maintenance | MoveLeader/Downgrade | 平台替代 | 分别由服务选主和 DBaaS 升级编排处理 |
| Endpoint | health/livez/readyz | 兼容核心语义 | `/health`、`/livez`、`/readyz` 及分项检查已对齐；`data_corruption`/`non_learner` 使用 TiKV 架构等价语义，`/ready` 与 `/ping` 为平台探针 |
| Concurrency | Lock/Election recipes | 兼容核心语义 | 官方 `client/v3/concurrency` Mutex/Election/session、orphan session lease 自然过期接棒及真实 Leader 故障转移已通过；继续长时间 soak |

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
  连续 10 轮通过。DeleteRange 的大范围/PrevKV 边界、Watch、Lease 和 Compact
  仍需逐组扩展差分矩阵。
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

### P1：通用服务能力

1. 继续扩大 Auth 差分、token/证书轮换和长连接故障验证；管理 API、key-range
   RBAC、token 生命周期、Watch/Lease 持续鉴权、客户端证书 CN 身份以及真实三副本
   auth+mTLS failover、服务端/内部客户端叶证书热轮换、CA trust pool 双信任窗口与撤旧
   及真实三副本在线轮换、长连接 drain/reconnect soak、CRL/cipher/TLS version 策略、
   独立 outbound client cert/key 与 peer CN/SAN allowlist 已完成；下一步扩大配额与故障
   注入覆盖。
2. `client/v3/concurrency` mutex/election/session、lease 自然过期和 failover
   recipe 已通过；继续增加长时间 soak。
3. 建立 DBaaS 控制面契约：创建、扩缩、升级、备份、恢复、证书轮换、销毁。
4. 建立实例级限额和计量：请求字节、txn 操作数、跨连接 client RPC 总并发、client
   请求 QPS/burst 及逻辑 Watch 总数已具备稳定错误与指标；继续补 CPU、内存、PV、
   备份容量、网络及容量计量。

### P2：运维兼容和长期验证

1. 明确 `etcdctl` 命令兼容表，为平台替代命令返回可操作提示。
2. 增加 BR/PITR 恢复演练、滚动升级、跨可用区故障、磁盘满和长时间 soak。
3. Porcupine 已覆盖无故障 Get/Put/CAS、多键 Txn 原子性和可表达不确定
   写结果的 KubeBrain Leader/TiKV/PD 故障历史；继续扩展 lease 模型，并在
   多 store/多 PD 预生产拓扑上做分区和多点故障注入。
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
