# 共享 PD/TiKV 上的多租户 GC —— 为什么不做隔离

**结论:共库多租户(多个 `--keyspace` 租户共享一套 PD/TiKV)不需要、也没实现 GC safepoint 隔离。要物理回收隔离 / 爆炸半径隔离,用独立 cell(每租户一套 PD/TiKV)。**

本页记录我们**考虑过并否掉**「per-keyspace GC safepoint 隔离」这条路,免得以后有人重提。相关调查见 memory `finding-76-gc-safepoint-fix-ineffective`。

## 背景:曾经担心的问题

共库多租户下,TiKV 只有**一个全集群 GC safepoint**。曾担心:一个配了短 `--storage-gc-lifetime` 的激进租户推进这个 safepoint,会回收另一个"保守"租户还需要的 MVCC 版本(cross-tenant stomp)。

为此做过两版尝试,都在 dbaas 上、已 **revert**:
- **v1**(每 keyspace 一条独立 PD service safepoint,靠 `UpdateServiceGCSafePoint` 返回的 min 来 clamp):真 PD 实测**无效**——PD 自留的 `gc_worker=0` 占位污染返回的 min,clamp 从不生效,激进租户照样推高集群 safepoint。
- **Option A**(推进前读 PD service-safepoint 全表、对 `>0` 真实地板取 min):真 PD 实测**能挡住 stomp**(集群 safepoint 落在最保守活租户)。

## 为什么最终否掉:这个 stomp 对 KubeBrain 无害

关键在 KubeBrain 的存储模型(见 `pkg/backend/storage_gc.go` 注释):

> KubeBrain 的 MVCC 编在自己的 key 里,**每次快照读都用当前时间戳(所有 Iter 传 ts=0)**,引擎层历史版本对它**纯属垃圾**。`--storage-gc-lifetime` **只需超过最长的单次快照(一次流式 List——秒级)**;默认 10m 已远超所需。

推论:

- KubeBrain **永不读历史版本**,需要的保留时长只是"最长 in-flight 快照"= **秒级**。
- 任何 per-tenant lifetime(5m / 1h / 7d)**都远超这个秒级需求**。
- 所以激进租户(5m)把集群 safepoint 推过保守租户(1h),回收的只是 **5m~1h 之间、没有任何 KubeBrain 租户在读的版本**。**保守租户的 1h "需求"本身是虚的**,stomp 删的是谁都不要的垃圾。

即:**Option A 保护的是一个在 KubeBrain 模型里不会造成伤害的场景**,净价值≈0,还要背 PD HTTP 依赖 / 冷启动竞态 / 每租户重复 GC 等真实成本。于是 revert。

> 这正是 `feedback-perf-stability-first` 说的「TiKV != etcd,别 cargo-cult」:"per-tenant GC lifetime 隔离"是 etcd/TiDB(真有 CDC/BR 读历史版本)的关注点,KubeBrain 不需要。

## 真外部消费者怎么办

如果哪天有**真的**读某租户 MVCC 历史的外部消费者(TiDB CDC changefeed、BR 备份),它们会**注册自己的 PD service safepoint**(不是 `gc_worker`、也不是 keyspace 名),而这类 service 早已被现有的 `clampGCTarget`(取 min across OTHER services)保护——**无需 KubeBrain 侧任何多租户 GC 改动**。

## 真需要隔离时:上 cell

要**各租户独立的物理回收节奏**、或**爆炸半径隔离**,唯一办法是把租户放进**独立 cell**——各自一套 PD/TiKV:

- 各 cell 独立的 TSO / region 调度 / GC safepoint;
- 一个 cell 的 PD 挂了只影响该 cell 的租户;
- 靠"加 cell"水平扩容,租户→cell 的路由/编排由上层控制面负责。

单套 PD/TiKV 的租户密度有上限(PD TSO 吞吐 / region 调度 / 每租户全键空间 GC 扫描的聚合背压),cell 数量没有上限。
