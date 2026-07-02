# P1 读放大 — 压测基线（改动前）

在改任何代码前用官方 etcd 客户端 + Prometheus 指标量化读放大，作为对照基线。
harness：`hack/etcd-client-compat/read_amp_bench_test.go`（`TestReadAmpBaseline`）。

## 方法
- 打**单个 leader pod**（port-forward）以干净归因后端存储往返：
  ```
  kubectl -n kubebrain-dev patch deploy kubebrain --type=json \
    -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--enable-storage-metrics=true"}]'
  L=$(for p in $(kubectl -n kubebrain-dev get po -oname|grep kubebrain); do \
      kubectl -n kubebrain-dev logs $p --tail=1500 2>/dev/null|grep -q "start leading" && echo ${p#pod/}; done)
  kubectl -n kubebrain-dev port-forward pod/$L 13379:3379 18080:8080 &
  cd hack/etcd-client-compat
  READ_AMP=1 ENDPOINT=127.0.0.1:13379 READ_AMP_METRICS_URL=http://127.0.0.1:18080/metrics \
    READ_AMP_N=2000 go test -run TestReadAmpBaseline -v -timeout 20m ./...
  ```
- 关键指标 `storage_iter_start`：后端 `KvStorage.Iter` 每次调用 +1。LIST 每返回一个 KV 触发一次 `GetEtcdMetadata`→一次 Iter，故"iter/返回KV"≈读放大倍数。

## 基线结果（N=2000，~1KB 值，page=500，TiKV 后端，2026-07-02）

| 操作 | 返回 | 墙钟 | 后端 Iter 次数 | iter / 返回KV |
|---|---|---|---|---|
| 单页 LIST (limit=500) | 500 | **9.99s** | 502 | **1.00** |
| 全量分页 LIST (2000, 4 页) | 2000 | **40.5s** | 2007 | 1.00 |
| CountOnly | 2000 | 61ms | 1 | — |
| 历史 LIST @rev (2000) | 2000 | 39.9s | 2007 | 1.00 |
| Watch 扇出 (20 watcher × 100 写) | 2000 事件 | 12.7s | 6300 | **3.15 / (写×watcher)** |

## 结论（对应审核项）
- **#3 / #7**：LIST/Get 每返回一个 KV 都多一次后端存储 Iter（取 create_revision/version 元数据），且在 TiKV 上每次 Iter 含一次取 TSO + 反向快照扫描 → **~20ms/键**。单页 500 键 = 10s；100k pod 的 LIST 外推 ≈ 2000s。这是"规模超越 etcd"目标的最大障碍。
- **#10 / #28**：watch 扇出每事件每 watcher ~3 次存储读（元数据 + prev-kv，含重试预算）。W 个 watcher × E 事件 = O(W×E) 存储读。
- **#5 / #27 / #29**：分页/历史 LIST 的精确 Count 与 CountOnly 走全量扫描（此规模下墙钟未爆，但 O(N²) 特性需更大 N 才显现；已在 harness 覆盖，后续放大 N 复测）。

## 目标（改动后用同一 harness 复测）
- LIST 的 `iter/返回KV` 从 ~1.0 降到 ~0（元数据内联或一次批量解析，消除 per-KV 往返）。
- 单页 LIST 墙钟从 ~10s 降到亚秒级。
- watch 扇出 `iter/(写×watcher)` 从 ~3 降到 ~0（事件产生时解析一次，而非每 watcher 重复读）。

## 改动后（B：LIST 批量元数据，commit c056438）

同一 harness、同参数复测：

| 操作 | 基线 | 改后 | 提升 |
|---|---|---|---|
| 单页 LIST (500) | 9.99s / 502 iters | **116ms / 3 iters** | ~85× |
| 全量分页 LIST (2000) | 40.5s / 2007 | **293ms / 11** | ~138× |
| 历史 LIST (2000) | 39.9s / 2007 | **306ms / 11** | ~130× |
| CountOnly | 61ms / 1 | 66ms / 1 | 不变 |
| Watch 扇出 (20×100) | 6300 / 3.15 | 6300 / 3.15 | **未变（另条路径）** |

`iter/返回KV` 从 ~1.0 → ~0.01。已解决 **#3** 与 **#7 的 list-page 半**。

## 改动后（B：watch 扇出合并缓存，commit 3ba2cc1）

在 `backendShim` 加按 (key,revision) 键的合并缓存 + singleflight，把 W 个 watcher 对同一事件的元数据/prev-kv 查询合并成一次：

| 操作 | 基线 | LIST 批量后 | watch 缓存后 |
|---|---|---|---|
| Watch 扇出 (20×100) | 6300 iters / 3.15 | 6300 / 3.15 | **500 / 0.25**（~12.6×，墙钟 12.7s→5.2s） |

已解决 **#10 / #28**，并补齐 **#7 的 watch 半**（#7 完整解决）。剩余 0.25 是"每事件一次"的共享成本。
**仍未解决**：#5/#27（精确 Count 全量扫描 O(N²)，此 N 未显现，需更大 N 复测）；#29（CountOnly 全量拉值）；#30（watch 历史回放惊群）；#69（扇出无前缀路由）；以及 A（写时内联元数据+迁移，可一并干掉 #6/#15 并把上面各项的常数成本进一步压到 0）。

## #5/#27 复现（N=20k）与结论

放大到 N=20000（page=500，40 页）复现分页 Count 的 O(N²)：

| 操作 | N=2000 | N=20000 | 增长（10× N）|
|---|---|---|---|
| 全量分页 LIST | 293ms | **10.5s** | ~36× |
| 历史 LIST @rev | 306ms | **10.7s** | ~35× |
| 单页 LIST（含1次 count 扫描） | 116ms | 433ms | ~3.7× |
| CountOnly | 66ms | 350ms | ~5× |

超线性坐实：`exactRangeCount` 对每页都全量扫 `[pageStart, end]` 算 Count（40 页 × O(N)），且流式读全部值。

**结论（重要）**：apiserver 用 `Count` 判 `hasMore = len(Kvs) < Count`（分页终止）且要求**精确剩余计数**（末页 `len==Count`）。`TestRangeLimitCountReportsTotalMatches` 有意固定了"带 limit 返回精确总数/剩余数"的 etcd 语义。因此：
- **无法**用 `More` 派生近似 Count 而不破坏该被测语义（试过，测试红）。
- keys-only 计数**不可行**：删除用 tombstone 值标记，计数需读值判活/墓碑（除非把活性编码进 key）。
- 所以**无索引时精确计数本质是 O(N)/页 = 分页 O(N²)**，无法外科手术式修复。

**正确修法 = approach A 级重设计**：维护"活键计数索引"（写时增减 per-range 计数）或把活性/create_rev/version 内联进 key/value 使 keys-only 计数可行。建议与 A（元数据内联 + 迁移，同时干掉 #6/#15）**打包**做。或由用户决策：接受近似 RemainingItemCount（alpha/装饰性）换取分页 O(N)（需改 TestRangeLimitCountReportsTotalMatches）。#29（CountOnly 流式读值）同源。

**本轮不改代码**（避免破坏被测的精确 count 兼容语义）；仅复现 + 记录。

## 改动后（A-core：元数据内联，commit ee55f24）

create_revision/version 内联进 value 信封（写时），读时 `kvToEtcdKv` 从信封解出（脱值+取 meta，零查询），旧数据回退 etcdmeta。混存 live 集群实测：

| 操作 | 基线 | 批量(#3) | **A-core 内联** |
|---|---|---|---|
| 单页 LIST(500) | 9.99s / 502 iters | 116ms / 3 | **96ms / 2 iters** |
| 全量分页 LIST(2000) | 40.5s / 2007 | 293ms / 11 | **283ms / 7** |
| 历史 LIST(2000) | 39.9s / 2007 | 306ms / 11 | **277ms / 7** |
| Watch 扇出(20×100) | 6300 / 3.15 | 500 / 0.25 | **400 / 0.20** |

LIST 元数据查询彻底归零（仅剩范围扫描自身）；新写不再产生 etcdmeta（#6/#15 增长停止）。兼容套件（value/metadata 往返）+ 混存回退全绿。**#3/#7 完整根治。** 剩余：#5/#27/#29（count，需 A-index）、live-PUT watch 事件的每事件一次共享读（可由 A-core-2 让 collector 给 PUT 事件也包信封而归零）、legacy etcdmeta compaction 回收、A-index。

## 改动后（A-index：内存态计数索引，commit e885b8d，--enable-count-index）

leader 内存维护带版本 key 索引（collector 有序维护 + 选主重建，本次 dev 重建 35803 keys/1.34s），Count/分页 count 走索引 O(range)、不扫存储：

| 操作 | 基线 | A-index (N=20k) | 提升 |
|---|---|---|---|
| 全量分页 LIST(20k) | 10.5s / 176 iters | **1.44s / 40** | ~7.3× |
| 历史 LIST(20k) | 10.7s | **1.33s** | ~8× |
| CountOnly(20k) | 350ms / 1 | **6.6ms / 0 iters** | ~53× |
| 单页 LIST(500) | 433ms | **50ms / 1** | ~8.6× |

**#5/#27/#29 根治**：精确 count 走内存索引（`[baseRev,readyRev]` 内 O(range)，零存储读），分页不再 O(N²)。正确性：索引 count == 扫描 count（单测跨增删改+固定历史 rev）+ 兼容套件（精确 count 断言）全绿。fallback 兜底：rev 超出 [baseRev,readyRev] 或索引溢出 → 回退扫描。剩余：A-core-2（watch live-PUT 信封）、legacy etcdmeta compaction 回收、#30/#69、增广 rank(O(log N)) 优化。

## 改动后（A-core-2：PUT/CREATE 事件值内联，commit 20a54c4）

collector 给成功的 PUT/CREATE watch 事件值直接包信封（写时已知 create_rev/version），transform 从信封解、不再每事件读元数据：

| watch 扇出(20×100) | baseline | 缓存(#10/#28) | A-core | A-core-2 |
|---|---|---|---|---|
| iters/(写×watcher) | 3.15 | 0.25 | 0.20 | **0.15** |

元数据读消除（~21× 总降）。**残余 0.15 = prevKv 读**：apiserver watch 用 WithPrevKV（watcher.go:464），prev 是上一版本值、当前写不携带，此读固有；仍由 prevCache 跨 watcher 合并（故缓存不能完全退掉，退掉会回涨）。metaCache 现仅作旧数据回退。
