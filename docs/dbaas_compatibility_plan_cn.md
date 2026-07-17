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
| KV | Range/Put/DeleteRange | 部分兼容 | P0：事务内排序、过滤、历史读、大范围删除原子性及独立 Put/Range/DeleteRange 差分已补齐；继续做大范围资源限制与故障验证 |
| KV | Txn | 部分兼容 | P0：缺失键 guard、范围 phantom guard、嵌套分支和 staged 单 revision 提交已完成；继续做官方客户端差分及 TiKV 故障验证 |
| KV | Compact | 兼容核心语义 | P0：继续对齐 logical/physical 行为、错误与异步 GC |
| KV | RangeStream | 兼容核心语义 | etcd 3.7 支持的 CountOnly/Limit/KeysOnly/默认排序已对齐；自定义排序与 revision filter 同 etcd 明确 Unimplemented |
| Watch | create/cancel/progress/history/prevKV | 兼容核心语义 | P0：用官方客户端做事件完整性、压缩、断线恢复和慢消费者测试 |
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
  value，并在 KubeBrain 重启后复读成功。commit-undetermined 和 PD/TiKV
  故障注入仍是 P0 未完成项。
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
4. 建立实例级限额和计量：请求字节与 txn 操作数已对齐 etcd；继续补 CPU、内存、PV、
   备份容量、网络、QPS、watch 数及容量计量，限额错误必须稳定且可观测。

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
