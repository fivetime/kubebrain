# Approach A — 读放大根治设计（待评审，未实现）

> 目标：根治读放大的**结构性根因**，让 KubeBrain 在规模上真正超越 etcd。
> 本文档只做设计，**不改任何存储代码**；涉及持久化 value 格式变更 + 迁移，需评审通过后再实现。
> 关联：`docs/read_amp_baseline_cn.md`（基线/复现）、`docs/audit_remediation_todo_cn.md`（#3/#5/#6/#7/#10/#15/#27/#28/#29）。

## 背景（两条底层事实）
1. **KubeBrain 故意无内存态 key 索引**（不像 etcd 的 treeIndex）：数据在 TiKV，内存不持有全部 key → 突破单机内存上限。**精确计数走全量扫描是此选择的直接后果。**
2. 每个 KV 版本的 `create_revision/version` 存在**独立的 `\x00kubebrain/etcdmeta/` keyspace**（每次写一条、按版本），且该 keyspace 在所有 compaction 边界之外（`\x00` 排在 `/` 前）→ **永不回收（#6/#15）**，且每次 LIST/watch 转换要额外读它（#3/#7/#10/#28）。

## 拆分为两部分（性质不同）

### A-core：元数据内联（可干净根治，无架构取舍）
把 `create_revision + version` **内联进对象 value**，彻底不再需要 etcdmeta 查询。

**新 value 信封格式**（仅 `EnableEtcdCompatibility` 模式）：
```
[ magic 4B = 00 6B 62 01 ("\x00kb\x01") ][ createRevision 8B ][ version 8B ][ 原始 value ... ]
```
- **无碰撞依据**：k8s 存的 value 恒以 `k8s\x00`(0x6b) 或 `{`(0x7b) 开头，tombstone=`tombstone`(0x74)，**都不以 0x00 开头**；magic 以 0x00 起头 + 4 字节，实际数据不会碰撞。（对非 k8s 的任意 etcd value，理论上极低概率碰撞——故仅在 etcd 兼容模式启用，并文档声明保留该 value 前缀。）
- tombstone 仍用原 `tombstone` 标记（不加信封），删除事件识别不变。

**写路径**：`create` 写 `(rev, 1)`；`update` 写 `(preservedCreateRev, version+1)`——backend 在 update 时本就已知/已计算这些（现在写进 etcdmeta，改为写进信封）；不再调用 `putEtcdMetadata`。

**读路径**：解出信封 → 直接得 create_rev/version + 原始 value，**零额外存储读**。若 value 无信封（旧数据）→ **回退**到现有 etcdmeta 查询（保证向后兼容，不需强制迁移）。

**#6/#15 收尾**：新写不再产生 etcdmeta；旧 etcdmeta 通过（本设计附带的）**把 etcdmeta/lease keyspace 纳入 compaction 边界**随对象版本一起回收。二者结合，内部 keyspace 不再无界增长。

**效果**：根治 #6/#15；把 #3/#7/#10/#28 的残余常数（含我已加的批量/缓存）直接**归零**（新数据无需任何元数据查询）。

**迁移**：
- 默认**免迁移前向**：新写内联、旧读回退。功能立即正确，etcdmeta 增长即刻停止。
- 可选**离线迁移工具**（`hack/`）：重写旧对象 value 为信封格式，之后即可删除回退分支与 etcdmeta。dev 集群可直接重建。

### A-count：#5/#27/#29（有根本架构张力，需决策）
精确计数要 O(log N) **必须有按 key 的索引**。三条路：
- **A-index（真正根治，推荐）**：内存里维护一个按 key 排序的索引，**只存 key + 最新修订号/活性，不存 value**。给出 O(log N) 精确计数 + rank，并顺带加速 watch/list。value 仍在 TiKV，故同内存能扛远超 etcd 的数据量——这才是"超越 etcd"的自洽故事。代价：写时维护、选主时重建、内存管理（key 量级，非 value）。
- **A-revkey**：不加索引，用修订键把"当前计数"做廉价（修 CountOnly 当前场景 #29）；历史分页计数仍 O(N²)，常数大幅下降。保 premise，但未真正根治分页 count。
- **A-approx**：`More` 派生近似 Count，O(N) 分页；RemainingItemCount 变装饰性；需改 `TestRangeLimitCountReportsTotalMatches`。语义有损。

**开放决策（待用户定）**：A-index vs A-revkey vs A-approx。（A-core 与此正交，可先行。）

## 分期与风险
1. **Phase A-core**（本设计主体）：内联 + 读回退 + etcdmeta 纳入 compaction。风险：value 格式（信封碰撞论证）、compaction 边界变更。**改前需评审信封方案 + 迁移策略。**
2. **Phase A-count**：按开放决策实现（A-index 是大特性，单独立项）。

## 验证
- 复用 `hack/etcd-client-compat/read_amp_bench_test.go`：A-core 后 LIST/watch 的后端 iter 应从当前的"每页一次批量/每事件一次"进一步降到 **0**（新数据）。
- 黑盒：`TestEtcdKeyMetadataAndCompare` 等确认 create_rev/version/PrevKv 不变；新旧格式混存正确。
- 崩溃/重启：混存回退路径正确；compaction 回收旧 etcdmeta。
- 真实 apiserver：consistent-list / watch 端到端。

## 现状
**A-core 已接线并验证**（commit 3fe2e28 codec + ee55f24 wiring）：写时内联、读时脱信封（etcd shim kvToEtcdKv + brain server + historyWatchEvents），免迁移（旧数据回退 etcdmeta）。实测 LIST 元数据查询归零、新写停写 etcdmeta。**剩余**：legacy etcdmeta 纳入 compaction 回收；A-count（用户已定=A-index，待实现）；A-core-2（collector 给 live-PUT 事件包信封，消除 watch 每事件一次的共享读）。

（历史设计说明保留于下。）

### 原始状态
**未实现。** 等用户评审 A-core 的信封 + 迁移方案、并确定 A-count 方向后开始编码。
