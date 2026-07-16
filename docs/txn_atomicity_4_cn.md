# #4 通用 txn 原子性 —— 设计与实现状态

## 当前状态（2026-07-16）

Tier 1 和 Tier 2 均已实现，本文件后续“现状与缺陷”记录的是修复前基线，不再代表当前代码：

- `backend.TxnApply` 使用一个 TiKV batch 提交全部用户写和 lease attachment，所有有效写共享一个 MVCC revision；CAS 冲突时整批失败，不会部分应用。
- 单键 compare 生成“精确 revision / 仍不存在” OCC guard；range compare 通过 `BeginRangeTxn` 写屏障排除扫描后的 phantom insert。
- `stagedTxnExecutor` 在固定 `baseRev` 上构造事务内有序视图，支持 Range、range delete、`IgnoreValue`、`IgnoreLease` 和 nested txn，最后将每键最终状态一次提交。
- etcd 的重复 put、put/delete 区间重叠等非法形状在执行前拒绝；不再依赖非原子顺序执行来处理合法的多操作事务。

验证证据：

- backend/server 聚焦测试与 `-race` 覆盖单 revision、CAS contention、缺失键 guard、range phantom、staged read-your-write、嵌套事务、range delete 和 lease attachment。
- `hack/etcd-client-compat/txn_atomic_test.go` 通过官方 `client/v3` 黑盒验证多写单 revision、事务内读顺序及重叠 range delete。
- `hack/etcd-client-compat/txn_differential_test.go` 在真实 TiKV/PD-backed KubeBrain 与 `/root/etcd` 参考服务之间比较响应、revision、PrevKV、过滤/计数、错误，以及 32 客户端并发 create-if-absent；并发场景两端均严格只有一个成功分支。

## 修复前基线与缺陷

`Txn` RPC（`pkg/server/etcd/kv.go:125`）分流：
- **快路径**（apiserver 实际走的）：`isCreate`/`isUpdate`/`isDelete`/`isCompareDelete` → 单个 backend CAS 原子操作（`Create`/`Update`/`Delete`/`CompareDelete`）。**原子、单 revision、正确。**
- **通用路径** `executeGenericTxn`（`kv.go:709`）：处理多 op 或任意 compare 的 txn。

通用路径的原子性缺陷（#4）：
1. **compare 非隔离（TOCTOU）**：`txnComparePaths` 用 `evalCompare` 做**普通读**（`backend.Get`）判定 success/failure 分支，之后才执行写。读与写之间、以及与其它并发写之间无隔离——两个并发通用 txn 比较同一 key 可能都通过、都写（丢更新）。
2. **每 op 各自 revision**：每个写 op 是独立 backend 调用，各自 `deal()` 分配 revision。etcd 语义下一个 txn 的所有 op 共享**同一** revision。这里 `Then(Put(a), Put(b))` → a 在 R1、b 在 R2，watcher/响应头看到不同 revision。而且 `backendShim.Put` 本身是 **Get-then-CAS 重试循环**（`backendshim.go:316`），进一步放大非原子性。
3. **部分应用**：op N 失败时，op 1..N-1 已提交，无回滚。

## 可达性

apiserver 的存储层只发快路径 txn（guaranteed-update = 单 compare+put、create = compare-mod-0+put、delete）。`executeGenericTxn` 只被**非 apiserver 的多 op / 任意 compare txn**触发（自定义 etcd client、controller 直连、smoke 测试）。故 #4 虽标 high、是真实 etcd-compat 缺口，但对 apiserver 关键路径影响小。

## 可用原语

`backend.DeleteRange`（`txn.go:253`）已示范**多 key 单 revision 原子写**：一次 `deal()` 取一个 revision → 一个 `BeginBatchWrite` → 每 key `CAS(revisionKey, newRev, oldRev)` + object 写 → 一次原子 `Commit` → `notifyBatch` 在该单 revision 发全部事件。存储层 BatchWrite 提供原子多 key 提交 + 每 key CAS。这正是实现真正 txn 所需的原语。

## 方案：backend 事务应用原语

新增 `backend.TxnApply(ctx, applySet)`：
1. 读涉及 key 的当前 revision（用于 CAS 的 oldRev 与 meta 版本号）。
2. `deal()` 分配**一个** newRevision。
3. 建**一个** batch：
   - 每 PUT：`CAS(revisionKey, newRev, curRev)` + `Put(objectKey, encodeValueWithMeta(value, meta))`（meta 版本递增/新建）。
   - 每 DELETE：`CAS(revisionKey, newRev, curRev)` + `Put(objectKey, tombstone)`。
   - （Tier 2）每 **compare key**：guard `CAS(revisionKey, curRev, curRev)` 断言自读取以来未变。
4. 原子 `Commit`。任一 CAS 失败（并发改动）→ 整批失败。
5. `notifyBatch` 在 newRevision 发全部事件。
6. **整 txn CAS-冲突重试循环**（模拟 etcd 对同 key 并发写的串行化），受 ctx + 内部 deadline 约束，退化为可重试 Unavailable 而非死循环——复用 `backendShim.Put` 已有的重试范式。

etcd 层 `executeGenericTxn` 改为：评估 compare 选分支 → 收集该分支的 put/delete/range/nested → range（读）在 txn revision 上执行 → 写交给 `TxnApply` 单 revision 原子应用。

## 实施分档（均已完成）

- **Tier 1（已完成）**：写 op（put+delete）单 revision 原子应用（修 #2、#3 缺陷），range 读使用事务内 staged view。
- **Tier 2（已完成）**：compare key 的 OCC guard CAS + 整 txn 重试；range compare 使用写屏障，修复单键 TOCTOU 与区间 phantom。

## 风险

关键写路径改动；需覆盖：并发同 key/跨 key、事件顺序与单 revision、nested txn、range-in-txn、lease 绑定、compat 套件（etcd client 真实 txn 语义）。Tier 2 的整-txn 重试还需防活锁。

## 剩余验证边界

事务实现不再有已知语义缺口。DBaaS 上线门槛仍应包含多副本 leader 故障发生在高并发通用 txn 中途的黑盒测试，以及 TiKV uncertain commit 注入；它们验证故障恢复和 ACK 边界，不改变本文件已实现的事务模型。
