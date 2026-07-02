# A-index 设计 — 内存态 key 索引（根治精确计数 #5/#27/#29）

> 目标：给 leader 一个内存态、按 key 排序、带版本的索引，使 `[start,end)` 在**任意修订号**下的**精确活键计数**为 O(log N + 命中区间)，根治分页 count 的 O(N²) 全量扫描。
> 状态：**设计待评审，未实现。** 关联 `docs/read_amp_approach_a_cn.md`、`docs/read_amp_baseline_cn.md`。

## 为什么必须"带版本"（诚实的架构结论）
apiserver 分页 LIST 在第 2 页起**固定 revision**（continue token 里编码），并用 `hasMore = len(Kvs) < Count` 判是否还有下一页、用 `Count` 算 RemainingItemCount。因此 `Count` 必须是**固定 rev 下 `[continueKey, end)` 的精确活键数**：
- 若返回**当前态**计数：页间若发生删除，固定 rev 的快照仍返回这些键，但当前计数变小 → `hasMore` 可能提前变 false → **apiserver 少读数据**（比报错更糟）。
- 若发生新增：当前计数变大 → 末页后 `hasMore` 仍 true → 请求空页 → 触发 apiserver `no results but more` 错。

所以当前态索引不够，**必须能在固定历史 rev 下计数** → 采用 etcd `treeIndex` 同类的**带版本索引**。

**内存代价（必须知道）**：带版本索引在内存里持有**全部活键 + 各键自上次 compaction 以来的修订号列表**（不含 value，value 仍在 TiKV）。这与 etcd 的 treeIndex 内存量级相当，即**每节点 key 数受内存约束，和 etcd 一样**。KubeBrain 仍在这些维度胜过 etcd：value/存储容量（TiKV 分片）、写吞吐、多副本水平读扩展——只是 key 数的单机内存上限与 etcd 同量级。这是自洽且诚实的"超越 etcd"边界。

## 数据结构
```
type keyIndex struct {           // 每个 user key 一份
    key       []byte
    revisions []indexRev          // 该键的修订历史（升序），compaction 时裁剪
}
type indexRev struct {
    revision  uint64
    tombstone bool                // 该修订是否为删除
}
type treeIndex struct {
    mu   sync.RWMutex
    tree <ordered-by-key>         // 按 key 排序：Go 无 stdlib 有序容器，选 google/btree 或 hashicorp golang-lru? 用 btree（go.sum 已有 google/btree? 需确认）或自带跳表 pkg/backend/ring 风格
    // 附带每层子树的“活键计数”以支持 O(log N) rank（Fenwick/augmented btree）
}
```
- **有序容器**：优先 `github.com/google/btree`（若已在依赖）——支持范围遍历；rank/count 用**增广 btree**（每节点缓存子树 key 数）或退化为区间遍历 O(命中键数)（对单资源前缀，命中 = 该资源键数，通常远小于总量，可接受）。**MVP 用区间遍历**（O(区间活键数)），增广 rank 作为后续优化。

- **在 rev 处判活**：某 key 在 rev r 下活着 ⟺ 它有 `revision ≤ r` 且该"世代"最新的 `revision ≤ r` **不是 tombstone**，且 ≥ compactRevision。count([a,b), r) = 遍历 [a,b) 内的 keyIndex，逐个判活并计数。

## 操作
- **Put(key, rev)**：`keyIndex.revisions append {rev, false}`（新建则插入 tree）。
- **Tombstone(key, rev)**：append `{rev, true}`。
- **Compact(rev)**：对每个 keyIndex，丢弃 ≤ rev 的历史（保留跨越 rev 的最新世代起点）；若最新是 ≤rev 的 tombstone，则整个 keyIndex 删除。
- **Count(start,end,rev)** / **CountAt**：区间遍历 + 判活。
- 全部在 `treeIndex.mu` 下；读用 RLock。

## 维护（数据来源 = 有序事件流）
KubeBrain 的 `collectStorageWriteEvents` 已**按修订号严格有序**处理每个已提交事件。**在这里喂索引**：collector 每消费一个 revision 的事件，就对该 revision 的 (key, verb) 更新 treeIndex（PUT/CREATE→Put，DELETE→Tombstone）。这天然保证索引与提交顺序一致、无并发写竞争（单 collector goroutine 写索引）。索引的"已知 rev" = collector 的 committedRevision（读计数用 ≤ 它的 rev，apiserver 读本就 ≤ committed）。

## 选主重建（leader-only）
- 索引**只在 leader 维护**。成为 leader（`OnStartedLeading`）时**从存储重建**：扫描对象 keyspace 的修订键（每键一条，含最新 rev + 删除标记）建立当前态；历史世代按需——MVP 重建**当前态 + 只保留 > 当前 compactRevision 的历史**（对刚过去不久的 pinned rev 足够；更早的 pinned rev 已被 compact，count 走 ErrCompacted 或回退扫描）。
  - 更完整：重建时也可扫对象版本键补齐 compactRev..current 的历史世代。启动成本 = 一次范围扫描（keys-only 优先）。
- **followers**：不建索引。follower 上需要精确 count 的读**代理到 leader**（已有 etcdproxy 历史读代理机制），或回退到扫描（当前行为）。

## 崩溃/一致性
- **存储（TiKV）是唯一真相源**；索引是可重建的缓存。
- leader 崩溃 → 新 leader 重建（选主重建路径）。
- 索引落后于提交：collector 稍滞后于 commit，索引"已知 rev" ≤ committed；对 rev > 已知 的 count 请求**等待/回退扫描**（少见）。
- **shadow 校验（上线期）**：feature-flag 下，count 同时走索引与全量扫描并比对，不一致则告警 + 以扫描为准。稳定后关闭扫描。

## 集成
- `backend.Count` / `exactRangeCount`（Revision!=0 分页 count）改为查询 treeIndex；索引不可用（未建/落后/超内存上限）时**回退当前扫描**（保证正确性不倒退）。
- CountOnly（#29）同样走索引。

## 内存与降级
- 上限可配（`--count-index-max-keys`）；超限则**禁用索引、回退扫描**（保证不 OOM，退回 O(N²) 但正确）。
- 指标：索引 key 数、内存估算、重建耗时、shadow 不一致数、命中/回退计数。

## 分期
1. **MVP**：treeIndex（区间遍历 count，无增广 rank）+ collector 维护 + 选主重建（当前态 + compactRev..current 历史）+ 回退扫描 + shadow 校验 + feature-flag。验证：harness 分页 LIST（N=20k）从 10.5s→亚秒；`TestRangeLimitCountReportsTotalMatches` 及所有兼容测试仍绿（精确 count 保持）；真实 apiserver 分页/删除中途分页正确。
2. **优化**：增广 btree 做 O(log N) rank；followers 本地索引或统一代理；A-core-2 复用索引给 watch。

## 风险
- 内存（有上限 + 回退兜底）。
- 重建耗时（大集群启动/选主时一次扫描——用 keys-only/修订键扫描降成本）。
- 一致性（单 collector 写 + shadow 校验 + 存储兜底）。
- 历史世代深度（compaction 裁剪 + 对已 compact 的 pinned rev 返回 ErrCompacted，与 etcd 一致）。

## 状态
**未实现。等评审通过后按 MVP 分期实现。**
