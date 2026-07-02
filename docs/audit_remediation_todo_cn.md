# KubeBrain 审核问题修复 TODO

> 来源：Fable 5 多智能体审核（73 条已确认问题）。本文件是**持久化进度清单**，做一个勾一个（`[x]`），抗会话压缩遗忘。
> 编号 `#N` = 审核确认清单索引；`file:line` 为大致位置。测试遵循 `docs/test_strategy_cn.md`：**真实消费端黑盒为主，内部单测只锁黑盒够不着的精确 bug**。

进度：**已修 34 / 73** — Critical 3/3 ✓，High 23/32，Medium 3/17，Low 5/21。（+ #5/#27/#29 A-index e885b8d；#6/#38 etcdmeta 纳入 compaction；#15 两半完成：\x00kubebrain/ 命名空间整体纳入 compaction；#30 watch history 去点读+限流；#69 前缀路由）
> 附带修复（不在 73 条内）：events-TTL 过期回收在多 border（etcdmeta / 多 SkippedPrefixes）下失效——`getTimeoutRevision` 会 drain 共享 compact-history 队列，原先每 border 各调一次，首个 border 耗尽旧记录后其余 border 拿到 timeoutRevision=0，静默关闭 `/events/` 过期。已改为每次 compaction 周期只计算一次并应用到所有 border（`scanner.Compact` 现接收全部 borders）。`TestCompactExpiredEvents` 覆盖。

## 已完成（PR #1: fivetime/kubebrain#1 + 81d36be）

- [x] **#0** [critical] Revision allocated but never committed when metadata read fails in update() — permanently stalls revision pipeline and freezes lists/watches  
  `pkg/backend/txn.go:400` — 81d36be
- [x] **#1** [critical] DeleteRange commit failure leaks an allocated revision (no notify) — same permanent revision-pipeline stall  
  `pkg/backend/txn.go:311` — 81d36be
- [x] **#2** [critical] Progress notify sends the global current revision while older events are still buffered, letting reflectors skip undelivered events  
  `pkg/server/etcd/watch.go:405` — 81d36be
- [x] **#8** [high] Slow-watcher drop in WatcherHub.Stream can deliver a later revision after silently skipping one (permanent event loss)  
  `pkg/backend/watcherhub.go:98` — 81d36be
- [x] **#11** [high] processEvents goroutine leaks permanently when a watch is cancelled with a full result buffer  
  `pkg/backend/watch.go:214` — 18068c6
- [x] **#12** [high] ListByStream runs the backend scan under context.Background(); aborted range streams leak scan goroutines, iterators and snapshots forever  
  `pkg/server/etcd/backendshim.go:658` — 18068c6
- [x] **#13** [high] Event collector goroutine busy-spins at 100% CPU when idle  
  `pkg/backend/backend.go:270` — fb677a3
- [x] **#14** [high] Lease state is node-local and never reloaded on leader failover: new leader immediately expires live leases and orphans newer ones  
  `pkg/server/etcd/lease.go:284` — cad00fe
- [x] **#18** [high] Lease state is only restored at process startup, never on leadership acquisition — failover deletes kept-alive keys and orphans newer leases  
  `pkg/server/etcd/server.go:75` — cad00fe
- [x] **#19** [high] Client-supplied huge revision triggers deal() drift-back error that consumes a revision and drops it — remote watch-freeze DoS  
  `pkg/backend/backend.go:246` — 81d36be
- [x] **#20** [high] Compact revision marker can move backwards: setCompactRecord no-ops on smaller revision but compaction proceeds and scanner unconditionally overwrites the compact key  
  `pkg/backend/compact.go:98` — 9bf7792
- [x] **#22** [high] collectStorageWriteEvents busy-spins at 100% CPU when idle and can never be stopped  
  `pkg/backend/backend.go:269` — fb677a3
- [x] **#24** [high] TiKV commit does not classify ErrResultUndetermined as ErrUncertainResult  
  `pkg/storage/tikv/batch.go:140` — dcd3207
- [x] **#26** [high] TiKV BeginBatchWrite with failed Begin() causes nil-pointer panic in Commit  
  `pkg/storage/tikv/tikv.go:195` — dcd3207
- [x] **#35** [medium] Plain Put can fail with 'put failed after retries' under contention — etcd Put never fails on conflict  
  `pkg/server/etcd/backendshim.go:355` — 8e3bf42
- [x] **#51** [medium] Event collector goroutine busy-spins at 100% CPU on every replica, including followers where it can never make progress  
  `pkg/backend/backend.go:259` — fb677a3
- [x] **#58** [low] Status.Version is the non-semver string "kubebrain", breaking kube-apiserver etcd feature detection and version-parsing tooling  
  `pkg/server/etcd/maintenance.go:45` — dc9170a
- [x] **#70** [low] Hardcoded 200 TiKV clients multiply PD load, region caches, and defeat TSO batching  
  `pkg/storage/tikv/tikv.go:36` — b6e65b3

---

## 待办（按优先级）

### P0 — Watch/写 正确性遗漏（最先做，属之前跳过的正确性缺陷）

- [x] **#9** [high] handleWatchEventOverflow reset races with concurrent notify appends, leaving poisoned slots that wedge event collection  
  `pkg/backend/txn.go:469` — 693c264
- [x] **#21** [high] Watch-overflow reset races with in-flight notify: unsigned wraparound re-triggers overflow and moves the committed revision backwards  
  `pkg/backend/txn.go:444` — 693c264

### P1 — 读放大（项目核心「规模超越 etcd」；大重构，建议独立 PR + load/soak 压测）

- [x] **#3** [high] Every List result KV triggers a separate storage round-trip for etcd metadata  
  `pkg/server/etcd/backendshim.go:490` — c056438
- [x] **#7** [high] Per-KV etcd-metadata and prev-KV storage lookups cause serial read amplification on every list page and watch event  
  `pkg/server/etcd/backendshim.go:854` — list-page half c056438; watch half 3ba2cc1
- [x] **#5** [high] Paginated LIST with more=true re-lists the entire range (full values) just to compute Count when Revision != 0  
  `pkg/server/etcd/backendshim.go:505`
- [x] **#27** [high] Paginated list recomputes exact Count with a full unlimited range scan on every page  
  `pkg/server/etcd/backendshim.go:483`
- [x] **#29** [high] Count executes a full parallel scan streaming all values just to count keys  
  `pkg/backend/scanner/scanner.go:122`
- [x] **#10** [high] Watch event translation performs synchronous storage reads per event per watcher (metadata + prev-kv with 200ms retry budget)  
  `pkg/server/etcd/backendshim.go:808` — 3ba2cc1 (coalescing cache; 3.15 -> 0.25 iters/(write*watcher))
- [x] **#28** [high] Watch fanout performs 1-2 storage reads per event per watcher, with up to 5x10ms retry stalls  
  `pkg/server/etcd/backendshim.go:734` — 3ba2cc1
- [x] **#30** [high] Watch history fallback scans every version under the prefix and does per-key point reads; thundering herd after cache reset  
  `pkg/backend/watch.go` — 两处根治：(1) **消除每-tombstone 点读**：`historyWatchEvents` 现按有序扫描（同一 userKey 版本升序）就地记录上一个存活版本，DELETE 的 prev-kv 直接取自扫描，不再 `b.Get(rev-1)`（后者本身是一次 limit-1 Iter）。单次 fallback 从 `1+D` 次扫描降到 `1` 次（D=窗口内删除数）。`TestHistoryWatchEventsNoPerTombstoneReads` 钉死：4→1 iter 且 CREATE/PUT/DELETE + prev-kv 正确。(2) **限流缓解惊群**：`historyScanSem`（并发上限 8）串行化 cache reset 后的重连扫描风暴，等待者尊重 ctx。黑盒 `TestWatchHistoryFallbackCorrectness` 验证从旧 revision 观察的重放流正确。
- [x] **#69** [low] Watcher hub fans every event batch to every subscriber with per-watcher filtering, allocations, and 10k-slot channels  
  `pkg/backend/watcherhub.go` — **前缀路由**：`AddWatcher(ctx, prefix)` 记录每个 sub 的前缀，`broadcast` 用 `batchMatchesPrefix`（首个匹配即命中，单资源批 O(1)）跳过批次中无该前缀键的 sub——不发送、不唤醒下游 goroutine、不分配（写集中在少数前缀时省下大量无谓扇出）。权威过滤仍在 `processEvents`（保持并行）。另将其双次 `filterByPrefix(filterByRevision(...))` 合并为单趟惰性分配的 `filterEvents`（无匹配则零分配）。`TestBroadcastRoutesByPrefix` 覆盖路由；空前缀=watch-all 保持全扇出。`route_skipped` metric 可观测。10k 缓冲保留（drop-slow-watcher 语义相关，且已可配）。
- [ ] **[opt] count-index O(log N) 增广 rank**（低优先级）  
  `pkg/backend/countindex/countindex.go` — 现 MVP 的 `Count` 是区间遍历 O(区间键数)；分页 LIST 每页 count 范围逐页缩小，累计 O(N²/页大小)，**但仅在内存、无存储**。k8s 单资源约 15 万键时整次 LIST 的索引遍历 ~50–100ms、被对象检索（O(N) 存储）淹没，故现实够用。仅当单资源达**百万级 + 高频分页**才值得（此时遍历达秒级 CPU，且持 RLock 会短暂阻塞写）。优化 = btree 节点缓存子树活键数使 Count 变 O(log N)，不改语义/存储。**风险**：增广计数需在 Apply/Compact/Reset 时正确维护每节点子树计数（含删除后 liveAt 变化），维护 bug 会让 count 错→apiserver 分页出错/漏数据；须扩充 brute-force 交叉校验 + shadow。收益/风险比在现规模下不划算，故推后。

### P2 — 内部 keyspace 无界增长（etcdmeta/leases 在 compaction 边界外）

- [x] **#6** [high] Etcd metadata keyspace (\x00kubebrain/etcdmeta/) is written per revision but lies outside all compaction borders — unbounded storage growth  
  `pkg/backend/etcdmeta.go:13` — 两步根治：(1) 新写入不再落 etcdmeta，改内联到对象值信封（ee55f24, A-core）；(2) `getCompactBorders()` 现把 etcdmeta keyspace 纳入 compaction 边界，旧的 per-revision 元数据被 GC。`TestCompactRetiresLegacyEtcdMetadata` 证明 5 版本→compact→1 版本且元数据读仍正确。
- [x] **#15** [high] Internal MVCC keys (\x00kubebrain/leases/, \x00kubebrain/etcdmeta/) lie outside compaction borders and grow without bound  
  `pkg/server/etcd/lease.go:30` — 两半均根治。把 `getCompactBorders()` 的 etcdmeta 专用边界**扩成整个保留命名空间 `\x00kubebrain/`**（`internalKeyspacePrefix`），一条边界覆盖 etcdmeta + leases + 未来内部 keyspace。lease record 每次 keepalive 重写产生的旧版本、revoke/expire 的 tombstone 现随 compaction 回收；latest（`loadLeaseRecords` 在 max revision 读）始终保留。`TestCompactRetiresLeaseKeyspaceVersions` 钉死 5→1；黑盒 `TestLeaseSurvivesCompaction` 验证 compaction 周期后 lease 仍存活、绑定键与 lease→key 绑定不丢、keepalive 仍可用。用户键均以 `/` 开头，`\x00kubebrain/` 前导空字节天然不冲突（compact/election 键是 `{prefix}/...` 原始键、无 magicBytes、不在对象键压缩范围内，安全）。
  > 附带发现（预存在，不在 73 内，未修）：**读路径不回填 `kv.Lease`**。`backendShim.kvToEtcdKv` 构造 mvccpb.KeyValue 时不设 Lease 字段（etcd 的 Get/Range 会返回附着的 lease ID）。server 层有 `keyLeaseIndex`（key→leaseID），但下层 backendShim 拿不到，需要向上打通。apiserver 核心流程未必依赖，属 compat 完整度缺口。已记录。
  > 附带候选（预存在，未修）：**keepalive 写放大**——每次 keepalive 一次 `backend.Put`（新 MVCC 版本 + 事件 + compaction 负担）；etcd 不在每次 keepalive 落盘（重启/换主按 granted TTL 重置 deadline）。改此需谨慎处理换主时的 deadline 语义，与 #15「无界增长」正交，单列。
- [x] **#38** [medium] Etcd metadata versions are never compacted or deleted — unbounded storage growth  
  `pkg/backend/etcdmeta.go:47` — 同 #6：etcdmeta keyspace 现随 compaction 回收旧版本（scanner 无 revision key/tombstone，只走 version-compaction 分支，保留 ≤compactRev 的最新版、删更旧版）。
  > 部署观察：把 etcdmeta 纳入边界后，**首次** compaction 需一次性回收 46h 积压的旧 etcdmeta + 追平先前被静默关闭的 `/events/` 过期，扫描耗时较大；dev 集群上共享 120s 预算的 smoke 在 compact 步超时（非 hang、非数据错误，watermark 在下一次更高 revision 的 compaction 自愈）。稳态 compaction 是增量的、快。
- [x] **[async-compact]** [附带根治] `backend.Compact` 在 RPC 内同步跑物理扫描，超大数据集单次可能超过 apiserver compact 上下文超时→每轮被取消→永不追平物理 GC。
  `pkg/backend/compact.go` — 新增 `CompactAsync`：**同步推进 logical 水位**（`setCompactRecord`，使 `≤rev` 读立即返回 compacted）+ 把物理版本 GC 扫描交给单个后台 compactor（`runCompactor`，`compactTriggerRev`/`compactSignal` 合并并发请求、`compactScanMu` 与同步路径串行不重叠）。etcd 层 `Compact` RPC 按 `Physical` 分流：`Physical=false`（apiserver 用）→ CompactAsync 快速返回；`Physical=true`（etcdctl）→ 同步 `Compact` 阻塞至扫完。`backend.Compact`（同步）保持不变，故 backend/brain/测试路径行为不变。`TestCompactAsyncAdvancesWatermarkSyncThenGCsInBackground` 钉死「水位同步、GC 后台」；`TestCompactAsyncCoalescesConcurrentRequests` 验证 20 并发无死锁、水位单调。**顺带修 memkv 预存在线程不安全**：`store.Get` 不加锁、与迭代器 `init` 的 skiplist sentry 写竞争（异步 GC 与前台读并发才暴露）→ 给 `Get` 加锁（-race 干净）。生产 TiKV client 本就并发安全。

### P3 — etcd 语义正确性

- [ ] **#4** [high] Generic txn path (executeGenericTxn) is not atomic: compares are plain reads and ops are independent writes  
  `pkg/server/etcd/kv.go:696`
- [ ] **#52** [low] Watch PUT events fall back to CreateRevision=ModRevision when prev-version lookup fails — updates misreported as creates and PrevKv dropped  
  `pkg/server/etcd/backendshim.go:750`
- [ ] **#53** [low] Range at Revision==1888 (GetPartitionMagic) is hijacked to return partition metadata instead of data  
  `pkg/server/etcd/kv.go:77`
- [ ] **#54** [low] Apiserver compact txn emulation is non-atomic: read-check-then-two-Puts allows concurrent compactors to both 'win' and can desync the version key  
  `pkg/server/etcd/kv.go:1064`
- [ ] **#72** [low] compact_rev_key transaction emulated non-atomically with two separate Puts and a read-then-write version check  
  `pkg/server/etcd/kv.go:1030`
- [ ] **#55** [low] Ring-cache miss (ret.low) cancels the watch as 'compacted' with a fabricated compact revision instead of falling back to storage history  
  `pkg/backend/watch.go:91`

### P4 — Lease 正确性 / 性能剩余

- [ ] **#16** [high] Event TTL is a hardcoded 3600s substring heuristic that ignores the granted lease TTL and matches unrelated keys  
  `pkg/backend/txn.go:72`
- [ ] **#17** [high] Every Put with a lease rewrites the whole lease key-list to storage under a global mutex  
  `pkg/server/etcd/lease.go:204`
- [ ] **#36** [medium] Expiry deletes the lease record before the attached keys and swallows delete errors, permanently orphaning TTL keys  
  `pkg/server/etcd/lease.go:296`
- [ ] **#37** [medium] Physical expiry of event keys is out-of-band: no DELETE watch event, no tombstone, no revision  
  `pkg/backend/scanner/scanner.go:570`
- [ ] **#56** [low] Followers serve LeaseTimeToLive/LeaseLeases from stale local state when etcd proxy is disabled  
  `pkg/server/etcd/lease.go:147`
- [ ] **#57** [low] Keepalive can revive a concurrently revoked lease's record in storage, and stopLeases is never called  
  `pkg/server/etcd/lease.go:268`

### P5 — 安全 / 部署加固

- [ ] **#33** [high] No TLS support at all for the KubeBrain->TiKV/PD data plane  
  `cmd/option/option_tikv.go:49`
- [ ] **#32** [high] pprof and metrics handlers exposed unauthenticated on the production client port and always-plaintext info port  
  `pkg/endpoint/endpoint.go:138`
- [ ] **#31** [high] Revision syncer falls back from https to plain http for leader /status sync  
  `pkg/server/service/revision/revision.go:340`
- [ ] **#34** [high] Production monitoring is entirely non-functional: ServiceMonitor matches no Service and alerts reference nonexistent metric names  
  `deploy/production/monitoring.yaml:12`
- [ ] **#49** [medium] PodDisruptionBudget silently dropped in both production manifests (missing --- separator)  
  `deploy/production/kubebrain.yaml:30`
- [ ] **#50** [medium] --client-cert-auth=true without --trusted-ca-file passes validation but verifies client certs against the system root pool  
  `pkg/endpoint/config.go:226`

### P6 — Proxy / Leader / Revision 健壮性

- [ ] **#23** [high] Client-caused context cancellation tears down the shared proxy client for the whole follower  
  `pkg/server/service/etcdproxy/etcd_proxy.go:249`
- [ ] **#41** [medium] updateClient is not serialized: concurrent callers leak clientv3 clients and stampede the new leader  
  `pkg/server/service/etcdproxy/etcd_proxy.go:144`
- [ ] **#47** [medium] Concurrent etcdProxy.updateClient calls overwrite e.client without closing it, leaking etcd client connections  
  `pkg/server/service/etcdproxy/etcd_proxy.go:174`
- [ ] **#42** [medium] Follower ignores json.Unmarshal error from leader /status and can set its read revision to 0  
  `pkg/server/service/revision/revision.go:321`
- [ ] **#43** [medium] singleflight lets a follower read join an already-in-flight revision fetch, breaking the read-index staleness bound  
  `pkg/server/service/revision/revision.go:148`
- [ ] **#39** [medium] No write fencing on leader loss: deposed leader keeps committing writes with stale lower revisions that the new leader's watch stream never emits  
  `pkg/backend/election/election.go:149`
- [ ] **#40** [medium] Data race on resourceLock.record/tso/lastVal: Describe() read from gRPC handlers vs election-loop writes  
  `pkg/backend/election/election.go:188`
- [x] **#60** [low] leaderElection.leader bool is read/written without synchronization  
  `pkg/server/service/leader/leader.go` — `leader` 改为 `int32`，回调 `atomic.StoreInt32`、`IsLeader()` `atomic.LoadInt32`。
- [x] **#68** [low] leaderElection.leader is a plain bool written by callbacks and read by IsLeader() from all RPC goroutines  
  `pkg/server/service/leader/leader.go` — 同 #60。**顺带（`-race` 实锤）**：`resourceLock`（`pkg/backend/election/election.go`）的 `record`/`lastVal`/`tso` 被选主 goroutine（`Get`/`Create`/`Update`）写、RPC goroutine（`Describe`）并发读，同样无同步 → 加 `sync.Mutex`（I/O 在锁外，仅护字段访问；`Get` 返回快照拷贝，调用方不再触碰被护字段）。全量 backend `-race` 从 **9 → 3**，消掉全部 KubeBrain 自有选主竞争。残留 3 个全在 vendored **client-go v11.0.1 的 `leaderelection` 内部**（`observedRecord`/`reportedLeader`，老版库自身非线程安全），仅由「双 elector」测试触发；生产只跑单 elector 且不调 client-go 的 IsLeader（用自有已同步状态），无竞争。彻底消除需升级 client-go，属独立大改，未做。
- [ ] **#61** [low] onStoppedLeading health callback sets SERVING instead of NOT_SERVING  
  `pkg/server/server.go:78`
- [ ] **#62** [low] Proxy LeaseKeepAlive opens a new gRPC stream per keepalive message and never drains it  
  `pkg/server/service/etcdproxy/etcd_proxy.go:378`
- [ ] **#63** [low] Proxy watch started at revision 0 silently loses events across a leader change  
  `pkg/server/service/etcdproxy/etcd_proxy.go:494`

### P7 — 存储引擎健壮性 / 杂项

- [ ] **#25** [high] TiKV reverse iterator returns first key without range-border check  
  `pkg/storage/tikv/iter.go:47`
- [ ] **#44** [medium] Badger Commit does not map badger.ErrConflict/ErrTxnTooBig to storage errors  
  `pkg/storage/badger/batch.go:138`
- [ ] **#45** [medium] CAS on missing key: TiKV returns ErrKeyNotFound while Badger/memkv return Conflict  
  `pkg/storage/tikv/batch.go:56`
- [ ] **#46** [medium] memkv reads are unsynchronized and can observe the iterator's sentry placeholder  
  `pkg/storage/memkv/skiplist.go:63`
- [ ] **#48** [medium] GetCompactRevision is an uncached storage read executed on every revisioned request  
  `pkg/server/etcd/kv.go:415`
- [ ] **#64** [low] Badger and memkv iterators ignore the snapshot timestamp parameter  
  `pkg/storage/badger/iter.go:43`
- [ ] **#65** [low] Badger PutIfNotExist wraps nil error on ValueCopy failure, turning a failed op into silent success  
  `pkg/storage/badger/batch.go:42`
- [ ] **#66** [low] Badger value-log GC is never run  
  `pkg/storage/badger/badger.go:34`
- [ ] **#67** [low] Metrics iterator wrapper counts EOF/cancel instead of fetched rows  
  `pkg/storage/metrics/store.go:154`
- [ ] **#71** [low] Compaction errors are fully swallowed after the compact revision is persisted, reporting success while garbage accumulates  
  `pkg/backend/scanner/scanner.go:197`
- [ ] **#59** [low] HashKV ignores the request context, using context.Background() for revision/compaction lookups  
  `pkg/server/etcd/maintenance.go:82`

---

## 流程约定
- 每修一条：改代码 → 黑盒消费端测试（etcd client / 真实 apiserver）→（必要时）内部单测证明修前失败 → `go test ./... -race` → 构建镜像 + kind load + rollout → 对 live endpoint 验证 → commit（`Co-Authored-By`）→ 回本文件把 `[ ]` 改 `[x]` 并标 commit。
- 大重构（P1 读放大）先搭 load/soak 压测再改，另起 PR。
- 对 etcd 语义有疑问查 `/root/kubernetes`、`/root/etcd` 官方源码，不猜。
