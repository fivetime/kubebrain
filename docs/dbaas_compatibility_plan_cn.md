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
| KV | Range/Put/DeleteRange | 部分兼容 | P0：事务内排序、过滤、历史读和大范围删除原子性已补齐；继续做独立 RPC 边界差分与大范围资源限制 |
| KV | Txn | 部分兼容 | P0：缺失键 guard、范围 phantom guard、嵌套分支和 staged 单 revision 提交已完成；继续做官方客户端差分及 TiKV 故障验证 |
| KV | Compact | 兼容核心语义 | P0：继续对齐 logical/physical 行为、错误与异步 GC |
| KV | RangeStream | Kubernetes 请求形状兼容 | P1：对齐 etcd 3.7 支持的通用请求形状，或明确返回 Unimplemented |
| Watch | create/cancel/progress/history/prevKV | 兼容核心语义 | P0：用官方客户端做事件完整性、压缩、断线恢复和慢消费者测试 |
| Lease | grant/revoke/keepalive/ttl/list | 部分兼容 | P0：隔离 lease meta/attachment 的用户 MVCC revision；补并发、故障转移、事务附着及错误矩阵 |
| Auth | 用户、角色、权限、token | 缺失 | P1：实现 etcd Auth API；DBaaS mTLS/IAM 不能替代客户端期望的 key-range RBAC |
| Cluster | MemberList | 兼容表面 | 返回 KubeBrain 服务成员信息 |
| Cluster | add/remove/update/promote | 平台替代 | 由 DBaaS 控制面扩缩 KubeBrain、PD、TiKV；RPC 保持明确 Unimplemented |
| Maintenance | Status | 部分兼容 | P1：返回真实服务身份、版本、leader/revision；容量转到实例指标 |
| Maintenance | Snapshot | 平台替代 | 使用 TiKV BR/PITR；控制面提供备份、恢复和导出任务，不伪造 etcd snapshot |
| Maintenance | Defragment | 平台替代 | TiKV GC/compaction 管理，不执行 bbolt 碎片整理 |
| Maintenance | Alarm/DbSize | 平台替代 | 用 PD/TiKV 容量、磁盘、region 和配额告警；etcd 专属字段保持可解释值 |
| Maintenance | Hash/HashKV | 部分兼容 | P2：确定客户端用途；需要逻辑校验时实现可分页、固定 revision 的内容摘要 |
| Maintenance | MoveLeader/Downgrade | 平台替代 | 分别由服务选主和 DBaaS 升级编排处理 |
| Concurrency | Lock/Election recipes | 兼容核心语义 | 官方 `client/v3/concurrency` Mutex/Election/session 及真实 Leader 故障转移已通过；继续补 lease 自然过期和长时间 soak |

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
- **新确认的 Lease revision 缺口（未解决）**：同一差分最初加入 leased key
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

### P1：通用服务能力

1. 实现 etcd Auth 用户、角色、key-range 权限和 token 生命周期，并与每实例
   mTLS 配合。详细设计见 `docs/dbaas_auth_design_cn.md`；在管理面、token、
   unary 数据面和 Watch 持续鉴权全部完成前，AuthEnable 继续明确返回
   Unimplemented，避免产生“已启用但数据面未保护”的安全假象。
2. 验证 `client/v3/concurrency` 的 mutex、election、session 失效和 leader
   切换语义。
3. 建立 DBaaS 控制面契约：创建、扩缩、升级、备份、恢复、证书轮换、销毁。
4. 建立实例级限额和计量：CPU、内存、PV、备份容量、网络、QPS、watch 数、
   value/txn 大小；限额错误必须稳定且可观测。

### P2：运维兼容和长期验证

1. 明确 `etcdctl` 命令兼容表，为平台替代命令返回可操作提示。
2. 增加 BR/PITR 恢复演练、滚动升级、跨可用区故障、磁盘满和长时间 soak。
3. 引入基于操作历史的线性一致性验证；大规模性能测试不能替代正确性证明。

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
