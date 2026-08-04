# KubeBrain 审核问题修复 TODO

> 来源：Fable 5 多智能体审核（73 条已确认问题）。本文件是**持久化进度清单**，做一个勾一个（`[x]`），抗会话压缩遗忘。
> 编号 `#N` = 审核确认清单索引；`file:line` 为大致位置。测试遵循 `docs/test_strategy_cn.md`：**真实消费端黑盒为主，内部单测只锁黑盒够不着的精确 bug**。

> **附带发现(超出 73,真 k3s 驱动挖出并已修，2026-07-03，commit 855fb73)**：**watch progress-notify 修订被钉死在起始值**。真 k3s v1.36(apiserver 用 KubeBrain 当外部 etcd)刷屏 `Too large resource version: <RV>, current: <冻结值>`,kube-controller-manager GC/resource-quota informer `WaitForCacheSync` 失败,**Deployment 永不生成 ReplicaSet**。根因:progress 报 `wt.syncedRev`(仅本 watch 已投递事件推进),安静 key range 无匹配事件 → 冻结在起始修订 → apiserver ConsistentListFromCache(k8s 1.31+)靠 progress-notify 推进的 watchcache RV 卡死。修法(ultracode 驱动 understand→design→implement→adversarial-verify;并发评审抓出一处 overflow reorder MAJOR 并修正):`WatcherHub.publishedRev` 已 fan-out 水位线 + 1s 周期性 **in-band progress marker**(Kv==nil 事件)投给所有 sub(含安静),经 shim 边界转 `WatchResult.ProgressRevision`(leader/follower 对称);marker 与事件同一 FIFO 通道,progress 绝不越过未投递事件;from-now watch 从 `GetPublishedRevision()` 播种。**黑盒(真 k3s,3 副本 TiKV)**:修后 Deployment ~2s 生成 ReplicaSet,`Too large resource version`/`Unable to sync caches`/quota+GC 同步超时从数百降为 0。-race 单测新增 quiet-watch/marker-ordering/follower-mapping/overflow 用例。

> **真 k3s 驱动阶段其余成果(2026-07-03,全部已合入 `main`,PR #2 merge `83f2dff`;PR #1 随之 MERGED;远端/本地只剩 `main`)**：
> - **Scanner 跨-partition List 复活删除键**(`03676a5`)：一个 user key 的多版本被 TiKV region 边界拆到不同 partition、且墓碑(最新)在另一 partition 时,List 会把删除的键当活键返回。根因=`adjustPartitionsBorders` 只对可 Decode 的边界对齐。修法=`coder.RevisionBoundaryForBorder` 对任意边界对齐到 user-key rev=0 起点。确定性复现 `TestScannerCrossPartitionTombstone`;真-TiKV 黑盒(删一半 List 精确无复现 + 196k 多-region List 精确)。
> - **大 DeleteRange 单事务失败**(`883006b`)：整个范围塞单个 TiKV 事务,几百键起 `cas failed`(k8s 逐对象删不触发,裸 etcd 客户端/运维 bulk 删中招)。修法=分块(128/块,每块原子;牺牲 etcd 全-单-revision 原子性,TiKV 事务有上限不可避免)+ `TestBackendDeleteRangeChunksLargeRange`。真-TiKV 黑盒:195,928 键单前缀删成功。
> - **毒键/MVCC 孤儿自愈**(`3a53134` + 工具 `a08643f`)：读得到却永久删不掉的键 = revision 索引键(rev=0 槽)缺失、对象键在;读扫对象键(可见)、写对索引键 CAS(缺失→永久冲突)。根因 = **已修的 #31 compaction/retry 竞态**。加 `backend.healOrphanIndex`(CAS 失败且索引缺失时重建索引再重试一次,纯修复不分配修订)+ `TestOrphanIndexSelfHeal`;`hack/tikv-persistence-smoke` 加 `--mode=dump|heal`。真集群黑盒:旧毒键 `cli.Delete` 现返回 `Deleted=1`(此前永远 0)、已清除。
> - **#4 progress-notify 小尾巴**(`c33160a`)：`--watch-progress-notify-interval`(默认 1s)贯通 backend marker + server 发射两处 ticker(经 BackendShim 共用同值,不再叠成 ~2s);`syncedRev==0` 时跳过无效的 `Header.Revision=0` progress。
> - **#5 测试卫生**(`b898f50`)：`TestReadPopulatesAttachedLease` 去抖(读前等 `GetCurrentRevision>=putRev`,memkv 修订异步推进,非真 bug);cmux / client-go election 两条 vendored `-race` 竞态用 build-tag `raceDetectorEnabled` 在 `-race` 下 `t.Skip`。**`go test -race ./pkg/backend/ ./pkg/server/etcd/ ./pkg/endpoint/` 现全绿。**
> - **压缩窗口(调查,非缺陷)**：KubeBrain **从不自动压缩**(`Compact`/`CompactAsync` 全客户端驱动,`runCompactor` 只做已请求的物理 GC);观测的"约 600 修订窗口" = kube-apiserver 自己的 5 分钟 compactor,正常 etcd 行为。
> - **#2 k3s 负载深度(已完成,104 核机)**：带 agent 真调度、events/pod-status/endpoints/node 心跳、单节点满载 soak(零泄漏)、failover-under-load、195k DeleteRange、3 副本 TiKV+PD 混沌(杀 store/PD leader 0.24–0.28% 瞬态、全恢复);**多节点(3 节点 k3s)**跨节点调度/网络/service/DNS + 多节点满载 soak + 混沌。脚本 `hack/dev/k3s-load-smoke.sh` 一键复跑。详见 `docs/production_readiness_cn.md` 顶部"真-k3s 消费端驱动验证进展"。
> - **仍属"未验证/需生产规模化"(非功能缺失)**:真正大规模(百千节点/10万+对象/持续高吞吐)、真分布式跨机跨AZ、过载对 lease 敏感客户端边界、数天长 soak、更广版本矩阵、**TiKV BR/PITR 备份恢复演练**(备份走 TiKV 原生,etcd Snapshot 刻意不实现=设计决定非缺口)。

进度：**已修 72 / 73** — Critical 3/3 ✓，High 32/32 ✓，Medium 17/17 ✓，Low 20/21。仅剩 1 项为显式**推迟**的 `[opt] count-index O(logN) augmented-rank`（非缺陷）。（+ #33 TiKV/PD 数据面 mTLS(config.UpdateGlobal，覆盖 TiKV RPC + PD safepoint-etcd，全有或全无校验) 276bada;#32 pprof/metrics 移出 client 端口+pprof 默认关+info 端口可 TLS 1fa98b8;#39 write fencing epoch 栅栏 8690161(ultracode);#59 HashKV 用请求 ctx;#23 proxy client-cancel 不拆共享 client b855f79;#31 revision syncer 禁 https→http 降级 b25454d;#50 client-cert-auth 必须配 CA 6ca7712;#34 监控修复 c499c1a;#49 PDB 分隔;P7 引擎一致性 #25/#44/#45/#46 c12d982;#43 read-index 双缓冲 fetch 013fd93;#42 follower 拒绝畸形/零 revision d697c62;#41/#47 etcdproxy updateClient 序列化 aab4b1a;#40 resourceLock 字段竞争=随 #60/#68 解决;#37 event 过期改走 lease in-band DELETE=随 #16 解决;#56/#57 follower lease 读不返陈旧态 + 降级 StopLeases d924106；#36 lease expiry 先删键后删 record 防孤儿 b616a9f；#17 lease per-key attachment 去 O(N²) key-list 重写 503c397；#16 event TTL 去硬编码启发式=删子串 GC 依赖 lease 9c60cf3；#71 compaction 错误可见性；#48 compact TTL 缓存；P7 #65/#66/#67）（+ #5/#27/#29 A-index e885b8d；#6/#38 etcdmeta 纳入 compaction；#15 两半完成：\x00kubebrain/ 命名空间整体纳入 compaction；#30 watch history 去点读+限流；#69 前缀路由；kv.Lease 读回填=Get/Range/watch 回填附着 lease + 无锁快路径 86e692d）
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
  > 附带发现（预存在，不在 73 内，**已修** 2026-07-02）：**读路径不回填 `kv.Lease`**。`backendShim.kvToEtcdKv` 构造 mvccpb.KeyValue 时不设 Lease 字段（etcd 的 Get/Range/watch event 会返回附着的 lease ID）。修法：给 `backendShim` 加 `leaseLookup func(key string) int64` + `SetLeaseLookup`，`New()` 里用 `server.leaseIDForKey` 打通 server 层 `keyLeaseIndex`；`kvToEtcdKv`（Get/Range/watch/prevKv 唯一收口点）回填 `out.Lease`。为避免读热路径每返回一个 KV 就抢一次 `leaseMu`，加无锁快路径：`leasedKeyCount`（atomic，镜像 `len(keyLeaseIndex)`，在 bind/unbind/removeLease/applyLeaseRecords 四处 locked 变更点维护）为 0 时 `leaseIDForKey` 直接返回 0 不加锁（无 leased key 的 range 是绝对常态）。**注意坑**：`defer atomic.Store(..., len(map))` 的实参在 defer 注册时（函数入口、bind 前）求值 → 存了旧值；必须用闭包 `defer func(){...}()`。`TestReadPopulatesAttachedLease`（Get/Range 回填 lease、plain 键 0、unbind 后回 0）+ `TestLeaseIDForKeyFastPath`（快路径计数随 bind/unbind 增减）。**黑盒已验证**（单副本 leader，`ENDPOINT=3379` etcd client：Grant→Put(WithLease)→Get/Range 均回 lease ID、plain 键 0、no-lease 覆盖后回 0 → LEASE_READ_OK）。**局限**：`keyLeaseIndex` 是 leader-only 内存态，follower 对**最新**读本地服务（仅历史读 `Revision>0` 才代理到 leader），故经 follower 的最新读回 Lease=0（与 [[#56]] follower 本地 lease 态陈旧同类）；leader 服务的读正确。
  > **A241 更正（2026-07-19）**：上述 follower `Lease=0` 局限已被后续 value
  > envelope v2 消除。每个 MVCC 版本的 lease ID 现与 value 一起持久化，
  > `kvToEtcdKv` 对新格式直接读取 inline metadata；`keyLeaseIndex` 只作为没有 inline
  > metadata 的 legacy raw value fallback。三个 Pod 独立 endpoint 连续 10 轮均返回
  > 正确 Lease，并在 revoke 后返回 TTL=-1。
  > 附带候选（预存在，**已修** 2026-07-02，多 agent workflow 驱动 understand/design/implement/adversarial-verify）：**keepalive 写放大**——原每次 keepalive `refreshLease → persistLeaseState → backend.Put`（新 MVCC 版本 + watch 事件 + TSO revision + CAS Get）；etcd `lessor.Renew` 只刷内存 expiry 不落盘。**两处必须一起改**（8ab5631）：① `refreshLease` 去掉 persist，keepalive 只 bump 内存 deadline + 重排 timer；② `applyLeaseRecords` 恢复 deadline 用 `now+grantedTTL` 而非持久化的 `DeadlineUnixNano`——因 keepalive 不再落盘，存储里的 deadline 永远停在 grant 时刻，若信它则换主/重启时会立刻过期一个被长期 keepalive 的活 lease（**回归 #14/#18**）；改用 now+TTL 镜像 etcd `initAndRecover + Promote→refresh`，从结构上杜绝该回归。grant/bind/unbind 的持久化不动（失败恢复 + #15 lease keyspace compaction 不受影响）；DeadlineUnixNano 仍写（记录格式稳定）但 restore 忽略。单测 `TestKeepAliveDoesNotWriteStorage`（keepalive 后 lease 记录 ModRevision 不变）/`TestReloadResetsDeadlineToGrantedTTL`/`TestKeptAliveLeaseSurvivesLeaderChangeWithFreshDeadline`；#14/#18 guard 仍绿；etcd 包 -race 干净。**黑盒**（TiKV 3 副本）：25 次 keepalive → 集群 revision delta=0（修前约 +25）、lease 仍活；TTL=20 的 lease 被 keepalive 撑过 grant deadline 后**删 leader pod**，新 leader reload 时用 now+TTL 续活、绑定键保留 → FAILOVER_LEASE_SURVIVED_OK。（注：未实现 etcd checkpoint/remainingTTL，换主后给满 TTL 窗口而非精确剩余——更保守、安全。）
- [x] **#38** [medium] Etcd metadata versions are never compacted or deleted — unbounded storage growth  
  `pkg/backend/etcdmeta.go:47` — 同 #6：etcdmeta keyspace 现随 compaction 回收旧版本（scanner 无 revision key/tombstone，只走 version-compaction 分支，保留 ≤compactRev 的最新版、删更旧版）。
  > 部署观察：把 etcdmeta 纳入边界后，**首次** compaction 需一次性回收 46h 积压的旧 etcdmeta + 追平先前被静默关闭的 `/events/` 过期，扫描耗时较大；dev 集群上共享 120s 预算的 smoke 在 compact 步超时（非 hang、非数据错误，watermark 在下一次更高 revision 的 compaction 自愈）。稳态 compaction 是增量的、快。
- [x] **[async-compact]** [附带根治] `backend.Compact` 在 RPC 内同步跑物理扫描，超大数据集单次可能超过 apiserver compact 上下文超时→每轮被取消→永不追平物理 GC。
  `pkg/backend/compact.go` — 新增 `CompactAsync`：**同步推进 logical 水位**（`setCompactRecord`，使 `≤rev` 读立即返回 compacted）+ 把物理版本 GC 扫描交给单个后台 compactor（`runCompactor`，`compactTriggerRev`/`compactSignal` 合并并发请求、`compactScanMu` 与同步路径串行不重叠）。etcd 层 `Compact` RPC 按 `Physical` 分流：`Physical=false`（apiserver 用）→ CompactAsync 快速返回；`Physical=true`（etcdctl）→ 同步 `Compact` 阻塞至扫完。`backend.Compact`（同步）保持不变，故 backend/brain/测试路径行为不变。`TestCompactAsyncAdvancesWatermarkSyncThenGCsInBackground` 钉死「水位同步、GC 后台」；`TestCompactAsyncCoalescesConcurrentRequests` 验证 20 并发无死锁、水位单调。**顺带修 memkv 预存在线程不安全**：`store.Get` 不加锁、与迭代器 `init` 的 skiplist sentry 写竞争（异步 GC 与前台读并发才暴露）→ 给 `Get` 加锁（-race 干净）。生产 TiKV client 本就并发安全。

### P3 — etcd 语义正确性

- [x] **#4** [high] Generic txn path (executeGenericTxn) is not atomic: compares are plain reads and ops are independent writes  
  `pkg/server/etcd/kv.go` + `pkg/backend/txn_apply.go`。设计见 `docs/txn_atomicity_4_cn.md`。
  - **Tier 1**：`backend.TxnApply` 把选中分支的多写 op（≥2、distinct key 的 put/单键 delete）**单 revision 原子应用**（仿 DeleteRange：一次 deal + 一个 batch 每 key `CAS(读到的原始字节)` + object 写 + 原子 commit + `notifyBatch`）；写-CAS 冲突有界重试模拟 etcd 无条件覆盖，非-CAS 提交错误发 invalid 事件让 collector 不 stall。修「每 op 各自 revision」「部分应用」。
  - **Tier 2（TOCTOU 隔离）**：compare 求值时对**存在的单键 compare** 捕获 OCC guard（`{key, 当前 revision}`），在同一 batch 里以**首位 no-op `CAS(revisionKey, R, R)`** 断言 compare key 未变。guard 冲突（compare key 变了）→ `ErrTxnGuardConflict`，etcd 层**重评 compare 并重试**（有界 deadline）；写冲突仍走 backend 内部重试。guard 与写 key **不相交**时才走原子路径（相交→回退）。使「compare→多写」可串行化。**残留**：absent compare key（无「断言不存在」CAS 原语）与 range compare 不 guard，仍是 Tier 1 语义（写原子、compare TOCTOU）——已记录。
  - 不支持形状（nested、range 读、单 op、IgnoreLease/Value、多键 range delete、compare∩write）**回退旧顺序路径，不回归**。
  - 测试：`TestTxnApply*`（单 rev 原子 / tombstone 重建 / no-op 不耗 rev / **guard 冲突不写** / 64 并发 distinct / 24 并发同 key CAS 重试无丢更新）；黑盒 `TestTxnMultiWriteSingleRevision`+`TestTxnCompareMultiWriteSingleRevision`（多写落单 rev + guarded 路径端到端）。backend+etcd 全套 + `-race` 干净；部署后全 smoke（各 txn/compare 形状）+ compat 绿。
- [x] **#52** [low] Watch PUT events fall back to CreateRevision=ModRevision when prev-version lookup fails — updates misreported as creates and PrevKv dropped  
  `pkg/server/etcd/backendshim.go` `watchEventToEtcdEvent` PUT 分支 — prevKv 查询失败时原**无条件**把 `CreateRevision=ModRevision`（IsCreate 误报 true）。改为：PUT 是 update 永不是 create，**优先用值内联的 create_revision**（approach A，kvToEtcdKv 已填），仅当未知（legacy 无内联/解码失败）才用 prevKv 推导，最后兜底 `ModRevision-1` 保证 `IsCreate()=false`；PrevKv 一律附带（查询失败则 nil，但不再污染事件类型）。单测 `TestWatchPutEventKeepsInlineCreateRevisionWhenPrevKvMissing`（内联 createRev 在 prevKv=nil 时保留）+ 更正 `TestWatchEventToEtcdEventRemainsUpdateWhenPrevKvUnavailable`（原测试固化了 bug）；黑盒 `TestWatchUpdateReportsUpdateNotCreate`（update 事件 IsCreate=false、createRev 正确、PrevKv 存在）。
- [x] **#53** [low] Range at Revision==1888 (GetPartitionMagic) is hijacked to return partition metadata instead of data  
  `pkg/server/etcd/kv.go` — 删除 etcd 层 `if r.Revision == GetPartitionMagic → GetPartitions` 劫持（**死代码**：经查 kubebrain-client v0.2.1 的 `concurrentRangeStream` 用 **brain 协议 `ListPartition` RPC** 取分区，从不发 etcd Range Revision=1888）。现 Range at 1888 走正常 etcd 语义（`checkRequestedRevision`→ compacted / future，或 List→空）。顺带删掉随之变死的 `backendShim.GetPartitions`（接口+实现）；保留 `GetPartitionMagic` 常量（range-stream List 的 EOF 事件 ModRevision 哨兵仍用）与 `backend.Backend.GetPartitions`（brain 协议用）。测试 `TestRangeAtMagicRevisionIsNotHijacked`（rev 1888 返回正常空 range 而非分区键）。
- [x] **#54** [low] Apiserver compact txn emulation is non-atomic: read-check-then-two-Puts allows concurrent compactors to both 'win' and can desync the version key  
  `pkg/server/etcd/kv.go` `compact()` — 重写为**用 compact_rev_key 自身的 MVCC version + 单次原子 Create/Update CAS**（version 0/缺失→Create，否则按当前 modRev Update）；compare 选 Then/Else，CAS 解并发竞态→**恰一个 compactor 胜**，败者 CAS 失败得到 Else 形状（当前 kv+version）。删除独立 `compactVersionKey` 计数器与 `getCompactVersion`。`TestTxnCompactRevisionConcurrentSingleWinner`（16 并发→恰 1 胜、最终 version 恰 2）+ 既有 `TestTxnCompactRevisionCAS`。
- [x] **#72** [low] compact_rev_key transaction emulated non-atomically with two separate Puts and a read-then-write version check  
  `pkg/server/etcd/kv.go` — 同 #54：原「两次独立 Put（compact_rev_key + compactVersionKey）+ 读-查-写」→ **单次原子 CAS**（一个 batch/一次 Create|Update）。升级过渡：apiserver 上次学到的 version 若与 compact_rev_key MVCC version 不一致（旧双胜 bug 曾致偏移），下一轮 compact 失败→apiserver 学到正确 version→一轮内自愈（apiserver 无 compaction 报错）。
- [x] **#55** [low] Ring-cache miss (ret.low) cancels the watch as 'compacted' with a fabricated compact revision instead of falling back to storage history  
  `pkg/backend/watch.go` — ret.low/ret.empty 已回退 `historyWatchEvents`（#30 及更早）；本次修**错误路径**：ret.low 原在 history 失败时无条件包成 `"cache event oldest revision ... newer than requested"`（含该子串→被 `isWatchCompactedError` 判为 compaction）→ **瞬时存储错误被误报为 compaction**、强制客户端伪 revision 重 list；ret.empty 则反过来把**真 compaction** 压成通用 "empty cache event"→客户端永久重试不重 list。两路径改为**直接透传 `historyErr`**：`historyWatchEvents` 在 rev<compactRev 时本就返回带 "compacted" 的真 compaction 错（客户端重 list），其余返回原始错（客户端重试）。测试：`TestBackendWatchHistoryFallbackRespectsCompaction` 加断言错误含 "compacted"；新增 `TestBackendWatchLowCacheTransientFailureNotCompaction`（ret.low + flaky Iter 瞬时失败 → 错误**不含** compaction 字样）。

### P4 — Lease 正确性 / 性能剩余

- [x] **#16** [high] Event TTL is a hardcoded 3600s substring heuristic that ignores the granted lease TTL and matches unrelated keys  
  `pkg/backend/txn.go:72` — 9c60cf3。原 `create` 用 `bytes.Contains(key, "/events/")` 给任意含该子串的键盖 3600s 硬编码 TTL,scanner 再按全局 compaction-history 超时 revision 物理删——三重错误:忽略实际 granted TTL、忽略 keepalive renewal、**误伤**任何路径含 `/events/` 的无关键(静默丢数据)。**根因**:etcd v3 无非-lease TTL;k8s apiserver 对每个 TTL 对象(event,TTL=event-ttl)`leaseManager.GetLease` 挂 lease(参考 `staging/.../etcd3/store.go:297`),而 KubeBrain server 层 lease 机制已按**实际 granted TTL** per-key 过期(`expireLease→backend.Delete`)。子串 TTL 是冗余+错误的重复。**修法(方向 A,用户确认)**:彻底删除依赖 lease——`create` 不再设存储 TTL(去掉 `events`/`eventsTTL` + `createWithMetadata`/`createBatchWithMetadata` 的 ttl 参数);scanner 删掉 `compactIfExpired` 的 `/events/` GC、`getTimeoutRevision`、`logCompactHistory`、`compactHistories` 队列(`compact.go` 删档)、`timeoutRevision` 贯穿、`Config.TTL`;compaction 只回收旧版本/tombstone(含过期键的 tombstone)。净 -232 行。`expire_test.go`(断言旧 GC)替换为 `events_no_ttl_test.go`(含 `/events/` 的键 compaction 后存活);实际 TTL 过期由现有 lease-expiry 测试覆盖。backend/scanner/etcd 套件绿。**黑盒(TiKV)**:5s lease 的 event 键按实际 5s 过期(非 3600s)、无 lease 的 `/events/` 路径键存活。(注:events-TTL 是 3600s 尺度机制,短窗黑盒不能廉价区分新旧;根治靠结构性移除+单测+ [[keepalive 写放大]]/#14/#18 已固化的 lease 机制。)
- [x] **#17** [high] Every Put with a lease rewrites the whole lease key-list to storage under a global mutex  
  `pkg/server/etcd/lease.go:204` — 503c397。原 attach 每次 `persistLeaseState` 把整个 lease 的 key-list 序列化成一条 JSON 重写;apiserver lease manager 把一个复用窗口内所有对象(数千 event)挂同一 lease → 每次 Create 重写不断增长的列表 = 热路径 O(N²) 写量。**修法(方向 A,用户确认)per-key attachment 记录**:拆成 meta `leases/<id>→{id,ttl}`(仅 grant 写)+ 每键附着记录 `leasekeys/<key>→id`(attach 一次小写、detach 一次删),attach/detach 变 **O(1)**。recovery 双 scan(meta + attachments)并 union 重建 key→lease;旧格式(meta 内联 keys[])在 leader 上 ReloadLeases 时**一次性迁移**成新格式(写 attachments + 重写无 keys 的 meta)。两 keyspace 均在 `\x00kubebrain/` 内受 #15 compaction 回收;`leasekeys/` 排在 `leases/` 前故 meta scan 不会误取 attachments;无 lease 的普通 Put 不写任何 lease 记账(hadPrevious 守卫)。单测 `TestLeaseAttachPersistsPerKeyNotWholeList`(25 次 attach meta 记录 revision 不变、每键一条 attachment、failover 全恢复、detach 精确减一)+ `TestLegacyLeaseRecordMigratesToAttachments`(旧内联记录 reload 后转成 attachments + 无 keys meta);现有 lease/failover guard 仍绿。**黑盒(TiKV)**:100 键挂同一 8s lease → TTL 到全部一起过期(100→0);attach 400 vs 800 键到同一 lease **~线性**(比值 2.15,旧 O(N²) 约 4×)。(注:预存在 flaky `TestReadPopulatesAttachedLease` 与本改动无关——诊断证明是 List 偶发返回 0 kv,非 lease/非 #17,HEAD 同率复现。)
- [x] **#36** [medium] Expiry deletes the lease record before the attached keys and swallows delete errors, permanently orphaning TTL keys  
  `pkg/server/etcd/lease.go:296` — b616a9f。原 `expireLease`/`revokeLease` 先 `removeLease`(删 meta+attachment+内存态)再删绑定键,且 expiry 用 `_, _ = backend.Delete` **吞错**:键删除失败/两步间崩溃 → 键无 lease 可指、永不再过期、永久孤儿。**修法(对齐 etcd revoke 顺序)**:先 `leaseKeysSnapshot`(不删 lease)拿键 → **先删所有绑定对象键** → 全成功才 `removeLease` 删 record+attachment。真实删除失败则**保留 lease + 重排 expiry timer**(`retryLeaseExpiry`)而非吞错;缺失键返回无错(backend.Delete 对 ErrKeyNotFound 返回 Succeeded=false, err=nil),故部分删除后重试幂等安全。`revokeLease` 把删除错误上抛给调用方并保留 lease。失败发 `lease.expire.delete.err` metric。单测 `TestExpiryKeepsLeaseAndRecordWhenKeyDeleteFails`(注入某键 Delete 失败→lease record 与两个绑定均存活;放开失败重跑→过期完成、键与 lease 全清);现有 expiry/revoke guard 仍绿。**黑盒(TiKV)**:40 键挂 6s lease → 过期后全删(40→0)、lease 消失(TTL=-1)、系统健康。
- [x] **#37** [medium] Physical expiry of event keys is out-of-band: no DELETE watch event, no tombstone, no revision  
  `pkg/backend/scanner/scanner.go:570` — **随 #16 一并解决**(无新代码)。该 finding 指的正是 scanner `compactIfExpired`/`compactKey` 用 `store.Del`/`DelCurrent` 裸删 `/events/` 键(不分配 revision、不写 tombstone、不发 watch 事件)——即带外物理过期。#16(9c60cf3)已**整段删除**这套 events-TTL 带外 GC,event 过期改走 server 层 lease 机制 → `expireLease → backend.Delete`,而 `backend.Delete` 会分配新 revision、写 tombstone(`newRevisionBytes` 末尾 0 标记删除)、并 `notify(..., proto.Event_DELETE)` 发出 watch 事件(txn.go:157)——完全 in-band。**回归护栏**:#16 的 `events_no_ttl_test.go` 断言 compaction 永不删 event 键,逼迫过期只能走 lease/backend.Delete in-band 路径,防止重新引入带外删。**黑盒(TiKV)**:put 键挂 5s lease、从 putRev+1 起 watch → lease 过期时收到该键的 **in-band DELETE 事件**(delRev=putRev+2 > putRev),证明消费端能看到过期删除。
- [x] **#56** [low] Followers serve LeaseTimeToLive/LeaseLeases from stale local state when etcd proxy is disabled  
  `pkg/server/etcd/lease.go:147` — d924106。原 `LeaseTimeToLive`/`LeaseLeases` 仅 `!IsLeader && proxy` 才代理,否则(含 **not-leader + proxy 关**)落到读**本地陈旧快照**(leader 通过 keepalive 推进 deadline、grant/revoke 的 lease follower 从未见)→ 返回错误 TTL/列表。改成与写类 lease RPC 同款:作为 leader 本地服务,否则代理,否则 `requireLeaseLeader` 返回 Unavailable。单测 `TestFollowerLeaseReadsDoNotServeStaleState`(follower+proxy 关→Unavailable,不返回注入的陈旧 lease)/`TestFollowerLeaseReadsProxyWhenEnabled`(proxy 开→转发)。
- [x] **#57** [low] Keepalive can revive a concurrently revoked lease's record in storage, and stopLeases is never called  
  `pkg/server/etcd/lease.go:268` — d924106。**(a)** keepalive 复活已 revoke 记录:已随 keepalive 写放大修复消失——`refreshLease` 不再落盘([[keepalive 写放大]] 8ab5631),keepalive 不写存储故无从复活。**(b)** `stopLeases` 定义但**从不调用** → 被降级的 leader 继续跑 expiry timer(churn)且持陈旧快照。接线 `onStoppedLeading → StopLeases`:丢领导权时停所有 expiry timer 并清空内存 lease/keyLeaseIndex/leasedKeyCount;重获领导权由 `ReloadLeases` 从存储重建。单测 `TestStopLeasesClearsSnapshot`(grant+bind 后 StopLeases 清空三者)。**黑盒(TiKV,proxy 开)**:删 leader pod 跨换主,`LeaseTimeToLive`(TTL+3 绑定键)与 `LeaseLeases` 仍正确——旧 leader 降级清态、新 leader reload lease 服务。

### P5 — 安全 / 部署加固

- [x] **#33** [high] No TLS support at all for the KubeBrain->TiKV/PD data plane  
  `cmd/option/option_tikv.go:49` — 276bada。`NewKvStorage` 只能明文建 txnkv client,即便 TiKV 集群开了 mTLS,所有 region RPC 与 PD safepoint-etcd 流量仍走明文。修法:加 `storagetikv.Security{CAPath,CertPath,KeyPath,VerifyCN}`,在建任何 client 前 `config.UpdateGlobal(NewSecurity(...))`——txnkv 对 **TiKV RPC client 与 PD safepoint-etcd client 都**读 `config.GetGlobalConfig().Security`,故一处生效覆盖整个数据面;空配置=明文=行为不变。经 `--tikv-ca-file/--tikv-cert-file/--tikv-key-file/--tikv-verify-cn` 打通 `storageConfig`,**全有或全无**校验(集群 TLS 是双向的,半套配置直接拒绝而非静默退回明文)。测试:`validate()` 全有/全无表 + flag 绑定。**未黑盒**:dev TiKV 集群跑明文,无法握手验证 TLS 路径;`sec.enabled()==false` 分支与旧建 client 逐字节相同(纯 no-op 增支),故跳过重新部署(避免又一次镜像构建撑爆磁盘=已知 wedge 风险),TLS 握手路径靠对照 vendored client-go(v2.0.1)源码 + 单测覆盖。
- [x] **#32** [high] pprof and metrics handlers exposed unauthenticated on the production client port and always-plaintext info port  
  `pkg/endpoint/endpoint.go:138` — 1fa98b8。client(数据)端口的 HTTP mux 向每个 etcd 客户端无认证暴露 `/metrics` 与 `/debug/pprof/*`(pprof=CPU/heap DoS+信息泄露);info 端口永远明文。修法:(1) `buildClientHttpServer` 不再注册 metrics/pprof,client 端口只留 client handler(health/ready)+ etcd gRPC;(2) pprof 改为 `--enable-pprof`(默认关)且**只**在 info 端口、绝不在 client 端口;(3) info/metrics 端口可经 `InfoSecurityConfig`(`--info-cert-file/--info-key-file/--info-trusted-ca-file/--info-client-cert-auth`)走 TLS,空配置保持明文=行为不变,并入 Validate。metrics 仍在 info 端口(ServiceMonitor 抓取处,#34)。**黑盒(TiKV)**:client 3379 对 `/debug/pprof/` 与 `/metrics` 均 404;info 8080 `/metrics` 正常(816 行)、`/debug/pprof/` 404(默认关)。endpoint config 测试绿。
- [x] **#31** [high] Revision syncer falls back from https to plain http for leader /status sync  
  `pkg/server/service/revision/revision.go:340` — b25454d。`getRetrySchemas` 启用 TLS 时返回 `[https, http]`,https 遇 schema 不匹配就**回退明文 http** → 攻击者可用明文伪造 leader /status、喂 follower 假 read revision(决定 follower 读在哪个 revision)。改为:启用 TLS 时**只用 https**、绝不回退明文;rollout 期间还在 http 的 leader 升级后须走 https 而非明文。测试:两个 "https->…http(原期望回退成功)" case 改为**期望失败**(`no suitable schema to leader`);非-TLS 的 http-only 与 https↔https case 不变;revision 套件绿。
- [x] **#34** [high] Production monitoring is entirely non-functional: ServiceMonitor matches no Service and alerts reference nonexistent metric names  
  `deploy/production/monitoring.yaml:12` — c499c1a。**(1)** ServiceMonitor selector 要 `name=kubebrain` **且** `component=peer`,但 `kubebrain-peer` Service 只有 name 标签 → 匹配不到任何 Service、什么都没抓。给 peer(及 client)Service 加 `component` 标签。**(2)** 告警引用了 KubeBrain 从不暴露的 metric 名:wrapper 把 `.`→`_` 且**不加 `_total`**(仅 grpc-prometheus 的带 `_total`)。修正:`write_total`→`write`(success=false)、`watch_event_buffer_full_total`→`watch_event_buffer_full`、`watch_backend_err_total`→`watch_event_buffer_stale_drop`(真实丢事件信号)、`leader_election_lost_total`→`leader_election_lost`;`grpc_server_handled_total`/`grpc_server_handling_seconds_bucket`/`watch_revision_lag` 本就正确。校验:ServiceMonitor 现匹配 peer Service 并抓 info 端口、所有告警 metric 名都能在代码 `EmitCounter/EmitGauge` 找到、三份 manifest 均解析通过。
- [x] **#49** [medium] PodDisruptionBudget silently dropped in both production manifests (missing --- separator)  
  `deploy/production/kubebrain.yaml:30` — (随 #34 批,提交于 PDB 分隔 commit)。`kubebrain.yaml`/`kubebrain-tls.yaml` 里 PDB 紧跟 Deployment 之间**缺 `---`** → kubectl 当成一个文档解析、PDB 被静默丢弃(生产无中断预算)。补 `---`;两份文件现各含独立的 PDB 与 Deployment(`yaml.safe_load_all` + `kubectl --dry-run=client` 均确认 PDB 与 Deployment 都被创建)。
- [x] **#50** [medium] --client-cert-auth=true without --trusted-ca-file passes validation but verifies client certs against the system root pool  
  `pkg/endpoint/config.go:226` — 6ca7712。`ClientAuth=true` 但无 CA 时,`init()` 跳过 CA 加载块(`ClientCAs` 保持 nil)却仍设 `ClientAuth=RequireAndVerifyClientCert` → Go 拿**系统根池**验客户端证书,任何公共 CA 签的证书都被信任,client cert auth 形同虚设。在校验处拒绝该配置(对齐 etcd `--client-cert-auth` 必须配 `--trusted-ca-file`)。单测 `TestClientCertAuthRequiresTrustedCA`(无 CA 报"trusted CA"错;有 CA 通过且 `ClientCAs` 指向配置的 CA 池而非系统根)。

### P6 — Proxy / Leader / Revision 健壮性

- [x] **#23** [high] Client-caused context cancellation tears down the shared proxy client for the whole follower  
  `pkg/server/service/etcdproxy/etcd_proxy.go:249` — b855f79。`isForwardConnectionError` 把 `context.Canceled/DeadlineExceeded`(及 gRPC 同类)当连接错误 → 任何调用方取消/超时自己的请求都会让 `markForwardError` 重置**共享**转发 client、拖垮该 follower 上所有在途转发。修法:把调用方 ctx 传给 `markForwardError`,`ctx.Err()!=nil`(调用方走了、非 leader 问题)时跳过重置;真正死掉的 leader 仍以 live-ctx 的 Unavailable 触发重置,且周期性 checkLeaderLoop 也会兜底。单测 `TestForwardErrorClientCancelDoesNotResetSharedClient`(client-cancel/gRPC-canceled + dead ctx 不重置;live ctx 的 Unavailable 重置);etcdproxy 套件 -race 绿。
- [x] **#41** [medium] updateClient is not serialized: concurrent callers leak clientv3 clients and stampede the new leader  
  `pkg/server/service/etcdproxy/etcd_proxy.go:144` — aab4b1a(与 #47 同一处)。`updateClient` 被 1s `checkLeaderLoop` 与每个 RPC goroutine(经 `waitReady`)并发调用,且在慢的 `clientv3.New`+`checkClientConn` 期间释放字段锁。加 `updateMu` 整函数持有 → 同一时刻只有一个 goroutine 建/换转发 client;胜者建好后排队者走 `hasClient()+checkConn()` 快路径直接返回不再重拨,消除对新 leader 的 stampede。锁序恒为 `updateMu→lock`(仅 updateClient 取 updateMu 且在 lock 前)无死锁。
- [x] **#47** [medium] Concurrent etcdProxy.updateClient calls overwrite e.client without closing it, leaking etcd client connections  
  `pkg/server/service/etcdproxy/etcd_proxy.go:174` — aab4b1a(见 #41)。序列化后不再有并发的 `e.client = client` 覆盖(旧 client 由 `resetClient` 先关),消除连接泄漏。单测 `TestUpdateClientConcurrentNoDeadlock`(updateClient 与取锁读者 hasClient/readyClient 并发跑,`-race` 查 updateMu↔lock 死锁/竞争);etcdproxy 套件 -race 绿。**黑盒(TiKV,proxy 开)已验证**:删 leader pod 跨换主,follower 经 proxy 的 `LeaseTimeToLive`/`LeaseLeases`(TTL+3 键)换主前后均正确——序列化 updateClient 让 proxy 干净重指向新 leader、无 hang/stampede,pods 保持 3/3。(注:验证途中一度误判"集群 wedge",实为**从 pod 内用了 `127.0.0.1:3379`(=pod 自身 localhost)** 的端点错误;pod 内须用 `kubebrain.kubebrain-dev.svc:3379`,host 才用 `127.0.0.1:3379`。见 [[dev-cluster-disk-wedge]] ENDPOINT GOTCHA。)
- [x] **#42** [medium] Follower ignores json.Unmarshal error from leader /status and can set its read revision to 0  
  `pkg/server/service/revision/revision.go:321` — d697c62。`getRevisionFromLeader` 忽略 `json.Unmarshal` 错误,body 畸形(LB/proxy 错误页、截断响应)时返回 `(0, nil)` → `SyncReadRevision` 调 `SetCurrentRevision(0)` 把 follower 读索引倒回 0、对空/倒退 revision 服务读。修法:检查 unmarshal 错误 + 拒绝零 revision(健康 leader 的 revision 是 TSO 派生、永不为 0,故 0 意味着 `{}`/错误 body);两者都返回 error 让 follower 保留原读 revision。归类为可重试(与非-200 同属瞬态),leader 短暂抖动在预算内重试而非立刻失败读。单测 `TestFollowerRejectsMalformedOrZeroRevision`:真实 `httptest` server 返回畸形/`{}`/零-revision body,驱动真实 revisionSyncer→`SyncReadRevision` 报错且 backend revision 保持 42(非 0);有效 revision(777)正常同步。(真实 HTTP 往返=等价黑盒,follower 端 parse 修复无需集群部署。)
- [x] **#43** [medium] singleflight lets a follower read join an already-in-flight revision fetch, breaking the read-index staleness bound  
  `pkg/server/service/revision/revision.go:148` — 013fd93。原 `singleFlightGetRevisionFromLeader` 用 singleflight 合并并发 follower 读到同一在途 fetch;中途到达的读会**加入已在跑的 fetch**、拿到一个在它到达**之前**读到的 leader revision → 漏掉其间已提交的写,破坏 read-index 陈旧上界(可线性化读漏读已提交写)。修法**双缓冲 fetch**:无在途 fetch 的读起一个新 fetch(读到的 revision 在它到达之后=新鲜);在途 fetch 期间到达的读加入 `next` 批,`next` 只在当前 fetch 完成后才启动(=在这些读到达之后)→ 新鲜;同一 fetch 期间到达的读合并成单个 next fetch(不 stampede leader),fetch 严格串行。fetch 用独立超时(受重试预算约束),单个读取消 ctx 不会中止其他读依赖的 fetch。单测 `TestReadIndexMidFlightReaderGetsFreshFetch`:阻塞首个在途 fetch、排入第二个读,断言其拿到第二次(更高)revision=新鲜 fetch 而非在途,总共恰好 2 次 fetch(合并保留);`-race` 绿。(旧 singleflight 下中途读会拿到在途的旧 revision→本测试会失败,故为真回归锁。真实 HTTP 往返=等价黑盒,follower 端并发修复无需集群部署。)
- [x] **#39** [medium] No write fencing on leader loss: deposed leader keeps committing writes with stale lower revisions that the new leader's watch stream never emits  
  `pkg/backend/fence.go` (new) — 8690161(**ultracode workflow** 设计+对抗验证)。原写 RPC 只在入口查 `IsLeader()`,之后分配 revision 并提交存储;领导权可在这最长 ~10s 窗口(含 backendShim CAS 重试环)内丢失 → 被废黜 leader 提交的写落在新 leader 事件采集器已越过的 revision → committed-yet-unwatched(apiserver 永不见=分裂脑/丢更新)。**epoch/freshness 栅栏**:leader.go 加 per-node 单调 leadership epoch(OnStartedLeading 里在 `leader=1` **之前**自增)+ 每次成功续租 stamp `lastRenewNanos`(`renewStampingLock` 包裹资源锁);`EpochAndLeadingFresh()` 返回(epoch, leader==1 且 last renew < `leadershipValidityBound`=5s)。kv.go 每个写入口捕获(epoch,fresh)、不 fresh 立拒、经 `WithLeadershipEpoch(ctx)` 下传 admit-epoch;backend `commitFenced` 在**每个** data `batch.Commit` 前重载(epoch,fresh),epoch 变或 freshness 过期就以 `ErrLeadershipFenced` 中止(不提交)→ 映射 `codes.Unavailable` 让客户端重试新 leader。5 提交点全接;fail-open(无栅栏=单节点/测试、无 epoch=内部写)。**安全**:`leadershipValidityBound`(5s)< `LeaseDuration`(8s),被隔离 leader 比继任者能获锁**早 ~3s 自我隔离** → 无双写重叠;epoch 相等还挡 lost-then-regained 的 ABA。窗口从 ~10s 收缩到 ~一次提交往返。**已知有界残留**(文档化、非回归):重检无法与持久提交原子,单次 `batch.Commit` 在通过重检**后**卡顿超 ~3s 余量(TiKV 写停/STW GC)仍可能落陈旧写;彻底封闭需 opt-in in-batch epoch CAS(写热 fence key、串行化写),默认不启(现成本 ~2 原子读/提交)。单测 `fence_test.go`(epoch 变/freshness 过期拒绝、fail-open、fenced Create 不落存储但采集器仍推进)+ leader epoch/freshness;全套件绿、-race 仅剩已知 client-go 选主 flaky。**黑盒(TiKV)**:稳定 leader 460/460 写全过(无误 fence);删 leader 跨换主 1400/1401 过+1 可重试 Unavailable、无丢写、恢复健康、新 leader 当选。  
  `pkg/backend/election/election.go:149`
- [x] **#40** [medium] Data race on resourceLock.record/tso/lastVal: Describe() read from gRPC handlers vs election-loop writes  
  `pkg/backend/election/election.go:188` — **与 #60/#68 同批解决**(见 #68 条目)。`resourceLock` 加了 `sync.Mutex`,`record`/`lastVal`/`tso` 的全部 13 处访问(`Get`/`getRecord`/`getTso`/`Create`/`Update`/`Describe`)均在 `r.mu` 内(存储 I/O 在锁外、仅护字段;`Get` 返回快照拷贝)。backend `-race` 从 9→3,消掉全部 KubeBrain 自有选主竞争;残留 3 全在 vendored client-go v11.0.1 `leaderelection` 内部(`observedRecord`/`reportedLeader`,非本 finding,需升级 client-go,见 [[known-flaky-tests]])。本条与 #40 是同一处修复,之前漏勾。
- [x] **#60** [low] leaderElection.leader bool is read/written without synchronization  
  `pkg/server/service/leader/leader.go` — `leader` 改为 `int32`，回调 `atomic.StoreInt32`、`IsLeader()` `atomic.LoadInt32`。
- [x] **#68** [low] leaderElection.leader is a plain bool written by callbacks and read by IsLeader() from all RPC goroutines  
  `pkg/server/service/leader/leader.go` — 同 #60。**顺带（`-race` 实锤）**：`resourceLock`（`pkg/backend/election/election.go`）的 `record`/`lastVal`/`tso` 被选主 goroutine（`Get`/`Create`/`Update`）写、RPC goroutine（`Describe`）并发读，同样无同步 → 加 `sync.Mutex`（I/O 在锁外，仅护字段访问；`Get` 返回快照拷贝，调用方不再触碰被护字段）。全量 backend `-race` 从 **9 → 3**，消掉全部 KubeBrain 自有选主竞争。残留 3 个全在 vendored **client-go v11.0.1 的 `leaderelection` 内部**（`observedRecord`/`reportedLeader`，老版库自身非线程安全），仅由「双 elector」测试触发；生产只跑单 elector 且不调 client-go 的 IsLeader（用自有已同步状态），无竞争。彻底消除需升级 client-go，属独立大改，未做。
- [x] **#61** [low] onStoppedLeading health callback sets SERVING instead of NOT_SERVING  
  `pkg/server/server.go` — 丢失 leader 时 `onStoppedLeading` 现设 `NOT_SERVING`（原误设 SERVING，会让健康检查客户端/LB 继续把非 leader 当 leader 路由）。顺带把两个内联回调抽成 `server` 的方法（`onStartedLeading`/`onStoppedLeading`）便于测试；`NewServer` 先建 `server` 再用方法值建 election。`TestLeadershipHealthTransitions` 钉死 NOT_SERVING→（获得 leader）SERVING→（丢失 leader）NOT_SERVING。
- [x] **#62** [low] Proxy LeaseKeepAlive opens a new gRPC stream per keepalive message and never drains it  
  `pkg/server/service/etcdproxy/etcd_proxy.go` — 每次转发用 per-call 可取消 ctx（`context.WithCancel`）+ `defer cancel()`，返回时把 leader 侧流**完全拆除**（原 `CloseSend()` 只半关、接收侧半开泄漏到长命 caller ctx 结束）。每消息一条新流是当前 unary 式接口的固有结构（fully 复用需重构接口，Low，未做）。经 follower 代理跑 lease 生命周期/keepalive/compaction 存活测试通过。
- [x] **#63** [low] Proxy watch started at revision 0 silently loses events across a leader change  
  `pkg/server/service/etcdproxy/etcd_proxy.go` — 两处：(1) watch options 加 `WithProgressNotify`，让 leader 在**空闲**时也周期广播其 revision；(2) 用 `nextWatchRevision(cur, wresp.Header.Revision)` 按响应头推进 `watchRevision`（事件响应头 ≥ 事件 ModRevision，故涵盖原按事件推进；空闲进度响应也能推进——正是原来 rev=0 watch 换主丢 gap 的场景）。后续 `969aa75` 已让 Created header 回填安全的 published revision，proxy 可立即建立 resume floor；A61 再将 leader from-now backend 注册改为从该 floor+1 replay，关闭 Created 发送与 `AddWatcher` 之间的同主丢事件窗口。`TestNextWatchRevision` 钉死推进/不回退/兼容旧 server 的 0 header；经 follower 代理跑 rev-0 watch + watch-history 测试通过。

### P7 — 存储引擎健壮性 / 杂项

- [x] **#25** [high] TiKV reverse iterator returns first key without range-border check  
  `pkg/storage/tikv/iter.go:47` — c12d982。反向迭代器用 `IterReverse(start+\x00)` 创建,**不受 `end`(下界)约束**;`Next` 的首次(`!moved`)分支直接返回 seek 落点、**跳过 `checkBorder`** → 空范围/首键已 ≤end 时会泄漏越界键。改为首键也走 `checkBorder`。单测 `TestReverseIterFirstKeyIsBorderChecked`(mock `tiKvIterator`:首键越界→EOF、在界内→返回、空迭代器→EOF)。
- [x] **#44** [medium] Badger Commit does not map badger.ErrConflict/ErrTxnTooBig to storage errors  
  `pkg/storage/badger/batch.go:138` — c12d982。`txn.Commit()` 原样返回裸 badger 错误。映射:`ErrConflict`(乐观事务冲突)→ `storage.ErrCASFailed`(可重试,对齐 TiKV write-conflict 映射);`ErrTxnTooBig` → 包裹的 `storage.ErrUnexpectedRet`(明确硬错、不泄漏 badger 抽象)。单测 `TestBadgerCommitConflictMapsToCASFailed`(确定性:b1 快照→b2 改同键提交→b1 CAS 提交冲突→ErrCASFailed)。
- [x] **#45** [medium] CAS on missing key: TiKV returns ErrKeyNotFound while Badger/memkv return Conflict  
  `pkg/storage/tikv/batch.go:56` — c12d982。CAS 缺失键=compare 前提(current==oldVal)不成立=compare 失败,按 storage interface 应返回 `ErrCASFailed`/`Conflict`(Conflict `Is` ErrCASFailed),而非 `ErrKeyNotFound`。否则同一情形在 TiKV 上是硬错、在 badger/memkv 上是可重试 CAS 失败。改 TiKV 返回 `NewErrConflict(idx,key,nil)`,与 badger/memkv 一致。单测 `TestBadgerCASMissingKeyIsCASFailed` 钉死期望行为(TiKV 改后代码同构;`*txnkv.KVTxn` 具体类型不可 in-process mock,故 TiKV 侧靠代码对齐 + 跨引擎期望)。
- [x] **#46** [medium] memkv reads are unsynchronized and can observe the iterator's sentry placeholder  
  `pkg/storage/memkv/skiplist.go:63` — **已在树中修复**(随早前 -race 工作);`Get` 与 `iter.init` 均持 `store.mu`,sentry 的插入+删除与结果物化全在锁内完成,并发 Get 永不见 sentry;`Next/Key/Val` 读不可变 buf 无需锁。补并发 Get/Iter/write 的 `-race` 测试 `TestConcurrentGetIterWriteNoRace` 钉死。所有 storage 包 `-race` 绿。
- [x] **#48** [medium] GetCompactRevision is an uncached storage read executed on every revisioned request  
  `pkg/backend/compact.go` — 加 backend 级 TTL 缓存（`compactRevCache`，1s）。compact revision 单调、仅经 `setCompactRecord` 前进，故短 TTL 安全：本节点推进水位时 `updateCompactRevCache` **即时刷新**（读立即拒绝新压缩区间）；否则 TTL 到期刷新一次（存储读从「每次带 revision 的请求一次」降到「~每秒一次」）。刷新在锁外做，慢的 compact-key 读不会 stall 所有带 revision 请求；`never lower` 保证单调。覆盖全部 caller（backend watch/etcd 层/brain/maintenance）。`TestGetCompactRevisionCachesAndUpdatesEagerly`（compact 后 10 次读 0 存储 Get、TTL 到期刷新 1 次）；部署后 smoke 的 compacted range/watch 仍正确拒绝（缓存即时反映压缩）。
- [~] **#64** [low] Badger and memkv iterators ignore the snapshot timestamp parameter  
  `pkg/storage/badger/iter.go` — **分析后暂缓（benign）**：badger/memkv 的 `GetPartitions` 只返回**单分区**，故一次 scan 只用一个迭代器 = 一个一致快照（badger read-txn 自带快照隔离），`ts` 的作用（多分区间一致快照）在单分区引擎上无意义。真正按外部 `ts` 读需 badger managed-mode + 把 KubeBrain revision 映射到 badger version（大改，非默认引擎，收益低）。已记录不修。
- [x] **#65** [low] Badger PutIfNotExist wraps nil error on ValueCopy failure, turning a failed op into silent success  
  `pkg/storage/badger/batch.go` — `ValueCopy` 失败时原 `errors.Wrapf(err, ...)` 的 `err` 在该分支为 **nil**（key 存在），`Wrapf(nil,...)` 返回 nil → 坏文件读被当成功。改为 `Wrapf(copyErr, ...)`。`TestBadgerBatchAndLifecycle` 覆盖 PutIfNotExist create/conflict 路径。**黑盒已验证**（2026-07-02，本地独立 `STORAGE=badger` 二进制 `bin/kube-brain --data-dir` 单节点，见 [[dev-cluster-disk-wedge]] 记录的 badger 独立跑法）：create-if-not-exist txn 连做两次 → 第一次 Succeeded=true、第二次 Succeeded=false（PutIfNotExist 正确返回 conflict 非静默成功）、值仍为 "first" 未被覆盖、create_rev==mod_rev 未被改动。注：确切 bug（坏 value-log 文件时 ValueCopy 失败仍返回 nil）是文件损坏 edge case，黑盒不可触发，由单测钉死；黑盒确认 PutIfNotExist 正常 conflict 路径健康。
- [x] **#66** [low] Badger value-log GC is never run  
  `pkg/storage/badger/badger.go` — 新增后台 `runValueLogGC`（每 5min `RunValueLogGC(0.5)` 循环至 ErrNoRewrite），`Close` 关 stop chan 干净退出。`TestBadgerBatchAndLifecycle` 验证 open→用→close 生命周期不 hang。**黑盒已验证**（2026-07-02，本地独立 badger 二进制）：GC goroutine 于 Open 启动；经 20 轮×200 key 的 write→delete→compact churn（last_rev=4021）server 保持健康、post-churn 读写正常、日志无 panic/fatal（logical version-GC scanner "skip gc revision key" 正常运行）；SIGTERM 4s 内 CLEAN_EXIT（Close→close(stop) 干净停 goroutine，未 hang）。注：5min 周期性 value-log 回收本身需长 soak 才触发一次 tick，由单测覆盖；黑盒覆盖 start→churn→clean-stop 全生命周期。
- [x] **#67** [low] Metrics iterator wrapper counts EOF/cancel instead of fetched rows  
  `pkg/storage/metrics/store.go` — `iterWrapper.Next` 原在 `else if err != nil`（即 EOF/cancel）分支 `counter++`，成功取行（`err==nil`）时不计 → `storage.iter.fetch.success` 计的是 EOF 而非取到的行数。改为 `err==nil` 时计数。影响全部引擎（含 TiKV）。`TestIterWrapperCountsFetchedRows`（取 5 行 → fetch.success=5）。**黑盒已验证**（2026-07-02）：`storage_iter_fetch_success` 随 range 行数**线性**增长（KubeBrain range 路径每 key 2 行）——TiKV live 集群 N=200→+400、N=800→+1600（per_row=2）；badger 独立二进制 N=300→+600。bug 版本一次 range 只会 +1（终态 EOF）与 N 无关；实测严格正比于行数即证明计的是取到的行数。
- [x] **#71** [low] Compaction errors are fully swallowed after the compact revision is persisted, reporting success while garbage accumulates  
  `pkg/backend/scanner/scanner.go` — `Compact` 原 `_, _ = r.scan(...)` 丢弃每个 border 的扫描错误（scan 已内部 backoff 重试，故丢的是持久错）。改为返回首个错误 + 逐 border 记 log；`physicalCompact` 捕获后**记 log + emit `backend.compact.scan.err` metric**，但**不**上抛（logical 水位已持久化=逻辑压缩成功，物理 GC 失败留下的垃圾会被后续更高 revision 的压缩回收）。使持久失败的物理 GC 可观测而非静默累积垃圾。`TestCompactSurfacesScanError`（注入 GetPartitions 失败→Compact 仍成功、metric 已 emit）。backend+scanner+etcd 全套绿。**黑盒已验证**（2026-07-02，dev 集群磁盘清理恢复后，见 [[dev-cluster-disk-wedge]]）：`hack/dev/compact-soak.sh` 30 轮×4 并发 latest-reader，真实 k8s `compact_rev_key` CAS 驱动（同时覆盖 #54/#72）→ 每轮 compacted_rev 追平 target、旧 rev 的 Range **和** Watch 均返回 `mvcc: required revision has been compacted`、168 次 latest-reader 全成功零失败；metrics：`backend_set_compact_revision=30`、`Compact` gRPC 30/30 全 `OK`、**`backend.compact.scan.err` 缺席**（物理 GC 全成功、#71 错误路径在健康集群正确未触发=行为中性无误报）。
- [x] **#59** [low] HashKV ignores the request context, using context.Background() for revision/compaction lookups  
  `pkg/server/etcd/maintenance.go:82` — (HashKV ctx 传播)。`checkRequestedRevision` 与 `GetCompactRevision` 原用 `context.Background()`,取消/超时的 HashKV 请求仍脱离调用方跑查找。改为用请求 ctx。

---

## 第二轮代码审查（review round 2，2026-07-03）

9 条独立审查意见，逐条核实属实性后分类处理。**接受并修**：#R1/#R2/#R6/#R8/#R9；**记文档、不改代码**（k8s 消费路径下安全、仅通用 etcd 语义未完整）：#R3/#R4/#R5。

- [x] **#R1** [med] Lease revoke/expire 用无条件 `Delete(rev=0)` 删键，可能删掉被并发 Put 重绑到另一 lease 的新值  
  `pkg/server/etcd/lease.go` — `06b46ba`。新增 `deleteLeasedKey`：删前 (1) 在 `leaseMu` 下复核 `keyLeaseIndex[key]==id`（已重绑/解绑→跳过）；(2) 读当前 modRev 做 **compare-delete**（keepalive re-Put 会失败→循环复核，重绑到别的 lease→不动）。`revoke`/`expire` 两处都改用它。`TestDeleteLeasedKeyCompareDeleteGuardsReassignment`（A→B 重绑后 A 的 revoke 不得删该键；正控：仍绑 A 的键必删）。
- [x] **#R2** [med] Put 与 attach 非原子、attach 错误被吞 → leased key 可能成永不过期的孤儿  
  `eedb5d7` — 见 #R9（同一根因，一并修）。leased 单 Put 现走单条原子 `TxnApply` 批（value + attachment 同批提交）；清 lease 时同批删 attachment。`TestLeasedPutWritesAttachmentAtomically`。
- [~] **#R3** [low] 通用 txn compare guard（范围比较 / 不存在键 guard）未完整支持 → **k8s 安全，未改**  
  k8s apiserver 的事务只用**单键 mod-revision 比较**（guaranteed_update 的乐观并发）；范围比较、`CreateRevision==0`「键不存在」等通用 etcd guard 形态 apiserver 不产生。当前 `TxnApply` 的单键 CAS guard（#4 Tier 2）已覆盖 apiserver 全部形态并保证可串行化。通用 etcd 完整 guard 属独立特性，收益低，未做。
- [~] **#R4** [low] txn 顺序回退路径对不支持的 txn 形态不保证完整原子性 → **k8s 安全，未改**  
  apiserver 的 txn 形态（单键 put/delete + 单键比较）走 `tryAtomicGenericTxn` 的原子批；仅**非 apiserver** 的多形态混合 txn 落到顺序回退（逐 op 提交、非单一 revision）。create 原子性由 `PutIfNotExist` 保证。对通用 etcd 客户端的完整多 op 原子性属独立改动，未做。
- [~] **#R5** [low] 大范围 DeleteRange 分块、非单一原子 txn → **by-design，未改**  
  `883006b`（分块）已落。TiKV 事务有大小上界（单 txn 不能无界大），故超大范围 DeleteRange **无法**作为一个原子 txn 提交；分块是存储层的固有约束而非缺陷。k8s 的 delete-collection 语义不要求整批跨键原子。详见生产就绪文档。
- [x] **#R6** [med] leader 就绪门控：`onStartedLeading` 先置 SERVING 再 reload lease，reload 失败仅记日志继续  
  `pkg/server/server.go` — `0ad81cf`。`ReloadLeases` 移到 `SetServing` **之前**并**失败重试**（ctx 可取消）到成功，节点绝不在 lease 状态未重建时对外就绪（否则 stale follower 快照的过期计时器会误删 keepalive 的 lease 或漏挂新授的）。`RebuildCountIndex` 仍留在 SetServing 之后（未就绪时 count 回退全扫、绝不给错值，故其失败不该门控就绪）。残留微窗见 commit（`leader=1` 在回调前置位）。
- [x] **#R8** [low] compact 水位 CAS 并发冲突被当错误返回（非幂等）  
  `pkg/backend/compact.go` — `44eebff`。`setCompactRecord` CAS 提交冲突时**回读**：若已存水位 ≥ 目标，按幂等成功返回（对齐 etcd 并发 compact 语义），仅真实落后才返回错误。`TestSetCompactRecordConcurrentCASNoError`（N=64 齐发并发 CompactAsync，修前 FAIL、修后 PASS）。
- [x] **#R9** [low] `KeyValue.Lease` 取自内存索引（当前绑定），非按 MVCC 版本 → 历史读/prevKv/delete 事件的 lease 不准  
  `eedb5d7` — lease 现按版本内联进 value envelope（新 v2 `\x00kb\x02`，未 leased 版本仍 v1 无体积回退）；读按版本回填、仅 legacy v1 回退到内存索引，无需迁移。串 create/update/TxnApply 与 watch 事件 meta。`TestLeaseInlinedPerVersion`（历史读报该版本的 lease 而非当前绑定）、`TestValueMetaV2Lease`（v1/v2 round-trip + 向后兼容）。同时根治 #R2：value 里已有权威 per-version lease，`deleteLeasedKey` 删前据此复核，杜绝 stale attachment 误删。

> 编号用 #R* 前缀以别于上文一轮审计的 #NN。#R2 与 #R9 同 commit（`eedb5d7`）；原子性覆盖 leased 单 Put（leased key 的主导路径：Events、masterlease endpoints），罕见的 generic-txn 路径仍按版本内联 lease（#R9）但 attachment 走 best-effort，由 `deleteLeasedKey` 的 per-version 复核兜底；range-delete 的 detach 保持 best-effort（残留 attachment 指向 tombstone，过期时为 no-op）。

---

## 第三轮代码审查（review round 3，2026-07-03）

5 条意见，核实后：**接受并修** #R10/#R11；**已在二轮闭环** #R12(=#R2/#R9)、#R13(=#R4)；**记文档、不改代码** #R14。

- [x] **#R10** [low] 历史 CountOnly 忽略请求 revision，按当前 revision 计数  
  `pkg/server/etcd/kv.go` — `969aa75`。快 Count 路径的守卫 `hasRangeRevisionFilters` 只看 `Min/MaxMod/CreateRevision`、**不看 `r.Revision`**；且 `CountRequest` proto（外部 `kubebrain-client` 模块）无 revision 字段。改：快路径再加 `r.Revision == 0` 门控，带 revision 的 CountOnly 落到 `List`（按 `r.Revision` 读快照、`applyRangeOptions` 在 CountOnly 时剥 Kvs 只留 Count）。**k8s 安全**：apiserver 的分页 count 走 List 响应的 `Count` 字段，不发带 revision 的 CountOnly；纯正确性修复。`TestCountOnlyHonorsRequestRevision`（rev3 后删 1 键 → 按 rev3 count 仍 3、按当前 count 2）。
- [x] **#R11** [low] from-now watch 的 created 响应 header revision 为 0  
  `pkg/server/etcd/watch.go` — `969aa75`。created `WatchResponse` 原发空 header；clientv3 用 created header revision 作为 from-now watch 的**恢复点**，created 后首事件前断线可能从 0 恢复而跳事件。改为回填当前已发布 revision（`GetPublishedRevision()`，即 seed progress-notify 用的那个安全下界，不会跳事件）。`TestWatchCreatedHeaderReportsCurrentRevision`（created header == published revision，非 0）。
- [x] **#R12** [med] lease attach/detach 未与用户 KV 写在同一原子事务、且 lease id 未内联进 value meta  
  **已在二轮闭环** = #R2 + #R9（`eedb5d7`）。lease id 已按版本内联进 value envelope；leased 单 Put 的 value+attachment 已同一 `TxnApply` 批原子提交、不再吞错误。审查建议的"废掉外置 `leasekeys/` 双写"**有意未做**：`leasekeys/` 是 failover 时 O(leased-keys) 重建索引的来源，废掉会逼 reload 全量扫描；现已是**单条原子写**而非双写，核心顾虑已消除。
- [~] **#R13** [low] generic txn fallback 仍非 etcd 单事务 → **k8s 安全，未改** = #R4。见上。
- [~] **#R14** [low] Hash/HashKV 是 revision hash、非 MVCC 内容 hash → **架构上基本 N/A，未改**  
  `pkg/server/etcd/maintenance.go` — `Hash`/`HashKV` 返回 `revisionHash(revision)`。etcd 的 HashKV 用于检测**多成员间 MVCC 内容分叉**；KubeBrain 是 TiKV 上的**单一逻辑副本**（复制/一致性由 TiKV raft 负责），无 etcd 式成员会分叉，该用途基本不适用；apiserver 也不调 HashKV。真做按内容 hash = 全量扫描 keyspace（代价大、收益低，仅对迁移校验/ops 工具有点用）。记为已知差异，暂缓，除非有 ops 校验需求再按需实现。

---

## 第四轮故障驱动发现（2026-08-04）

- [x] **#R15** [high] PD/TiKV 联合故障跨 KubeBrain 换主时，已建立 Watch 会被取消或永久静默停滞
  `pkg/server/etcd/watch.go`、`pkg/server/service/etcdproxy/etcd_proxy.go`、`pkg/server/server.go` — `6813010b`。真实 RED：保持一个从显式 revision 开始的 clientv3 Watch，同时持续 Put，并发删除当前 PD leader 与 leader-heavy TiKV；写恢复且最终线性 Range 可见已提交键，但 Watch 30s 内追不上，也不一定返回错误。日志证明有两个相邻缺口：(1) Watch 若建在旧 KubeBrain leader，本地 collector 随领导权结束而停止，但旧 subscriber 原先不关闭，形成 open-but-silent stream；(2) Watch 若建在 follower，代理跨换主的 `readyClient` 在 2s 暂时无可用 leader 后直接 `return`，关闭代理输出，入口 RPC 随即发 terminal Cancel。修复：(1) `onStoppedLeading` 调 `Backend.CloseWatchers` 退休旧 term 的本地订阅；RPC 从 `syncedRev+1`（只在 Send 成功后推进）在当前本地 leader/代理无缝续接，保证无 gap、无 duplicate，并为 leader 创建的认证 Watch 预先保留可转发凭证；(2) proxy Watch 将 no-leader/未就绪视为长流正常 failover，保留显式 resume revision，以 100ms 退避持续重试到继任者可用或调用方 ctx 结束，不再把短暂 Unavailable 转成 Cancel；发送代理结果也改为 ctx-aware，避免取消后阻塞。确定性测试 `TestLeaderWatchResumesThroughProxyAfterLocalGenerationCloses` 钉死 local rev10→proxy rev11 且无 Cancel，`TestLeadershipHealthTransitions` 同时断言丢主关闭订阅；相关用例 `-race -count=10` 绿。黑盒测试 `TestWatchDeliversCommittedWritesAcrossBackendFailover` 以最终线性 Range 为提交态 oracle，要求每个提交键 value/mod-revision 一致、恰好交付一次、事件 revision 严格递增；修复中间版 3 轮 soak 仍 2 轮 RED（定位 proxy 窗口），最终版连续 **5/5 GREEN**：每轮 42–44 个提交事件、5–8 次预期瞬态写失败、20–30s 故障恢复窗口，无 Watch Cancel/缺失/重复，总耗时 117.315s。运行镜像 `kubebrain:a3510b-proxy-watch-retry`，digest `sha256:58d43f3a4220fc155e5adb05b0168c344f187b6eb04eb7a46db3ca872cd831a1`；KubeBrain/PD/TiKV 均 3/3 Ready、0 restart。
- [x] **#R16** [high] follower 在 Watch Created 与 successor proxy 就绪之间失去旧 leader 时，创建会被同步 readiness 错误转为 Cancel
  `pkg/server/service/etcdproxy/etcd_proxy.go` — `d8581a1a`。确定性 RED 将 leader 固定为不可达地址：旧 `Watch` 同步消耗完整 2 秒 readiness budget 后返回 `Unavailable`；入口此前已经发送 Created，因此该暂时选主窗口会成为客户端可见的 terminal cancellation。A3511 移除创建路径的同步 `readyClient`，立即返回活跃输出流，由后台 failover 循环按显式 revision 等待 successor，直至 caller context 取消。单测要求返回小于 100ms、无 leader 时流保持打开、取消后及时关闭；定向 `-count=20` 与 race `-count=10` 通过。新增官方 clientv3 黑盒门禁直连 follower，在 Put 已明确进入 PD leader + leader-heavy TiKV 联合故障窗口后才创建 Watch，恢复后要求同一流交付精确探针。连续 5/5 通过，确认 outage 后流保持 3.13–25.70 秒，无 Cancel；运行镜像 `kubebrain:a3511-watch-create-failover`，ID `sha256:29eb60955d37e5ef25937770006032310d5ef39cb490c600e97e9e6b2ec8e8d7`，三层均 3/3 Ready、0 restart。

---

## 流程约定
- 每修一条：改代码 → 黑盒消费端测试（etcd client / 真实 apiserver）→（必要时）内部单测证明修前失败 → `go test ./... -race` → 构建镜像 + kind load + rollout → 对 live endpoint 验证 → commit（`Co-Authored-By`）→ 回本文件把 `[ ]` 改 `[x]` 并标 commit。
- 大重构（P1 读放大）先搭 load/soak 压测再改，另起 PR。
- 对 etcd 语义有疑问查 `/root/kubernetes`、`/root/etcd` 官方源码，不猜。
