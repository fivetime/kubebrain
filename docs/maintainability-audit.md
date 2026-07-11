# KubeBrain 可维护性审计报告

> **清理进度**(2026-07-11 起,逐波执行,每波 `-race`+全量测试为闸门):
> - ✅ **Wave 1 死代码+陈旧注释(D)**:删 creator 死包(D1)、pprof 盲导入(D2)、evalCompare+内嵌 mutex(D3)、修误导注释群(D4:compact/count/scanner/watcherhub/memkv/interface)。**D5 #NN 标签有意保留**(团队 memory 系统靠其可追溯,批量剥离高 churn 低价值)。
> - ⏳ Wave 2 小重复(B7-B10,C3)/ Wave 3 中等重复(B1-B6)/ Wave 4 惯用法(E)/ Wave 5 层泄漏(C1-C2)/ Wave 6 神对象(A1-A5)


## 一、执行判断(诚实结论)

**这不是屎山,而是一套结构基本健全、但在冲刺规模战役中积累了明确技术债的代码库。** 关键证据是:包分层方向是干净的(endpoint→server→backend→storage 无反向依赖,backend 不 import etcd proto),核心热路径注释密度高且多在讲"为什么",且刚通过 review #51 的严格 bug 审查、跑到 3333 万 key 规模——这些都不是屎山会有的特征。真正的债集中在三处**神对象**(`RPCServer`+内嵌 lease 子系统、`backendShim`、`kv.go` 的 Txn 引擎),以及一批**战役期"就地复制修复"留下的重复代码**(count 三段式解析、notify/invalid ring 填充、compare 真值表、跨后端 BatchWrite 契约各写一遍)。此外还有真正误导性的**死代码/陈旧注释**——尤其是看起来"承重"实则零调用的 `creator` 包,和与 #32 设计直接矛盾的 pprof 盲导入。这些都不是 bug,但每一处都强迫维护者"改两三处才对齐"或"逆向工程一个自己看不到的 review 编号"。总体评级:**健全底盘 + 可控债务**,应主动清理而非重写。

---

## 二、按主题分组的债务清单

### 主题 A:神对象 / 超大文件(最高维护痛点)

| 项 | 文件+符号 | 为什么痛 | 重构 | 工作量 |
|---|---|---|---|---|
| A1 | `pkg/server/etcd/server.go` `RPCServer`(51-84)+ 整个 `lease.go`(~975L) | 一个 struct 实现 KV/Watch/Maintenance/Auth/Cluster/Lease 六个服务,**并直接内嵌 lease 管理器全部私有状态**(leaseMu/leases/keyLeaseIndex/leasedKeyCount/orphanSweepStop);kv.go 的 Txn/Put/DeleteRange 直接 inline 调 bind/unbind,lease 状态机从写热路径被改动,毫无封装边界 | 抽出 `leaseManager` 类型独占所有 lease 状态与 lease.go 方法,RPCServer 只持一个 `*leaseManager`,写路径调 `leaseMgr.Bind/Unbind/IDForKey` | large |
| A2 | `pkg/server/etcd/backendshim.go`(1253L)+`backendshim_cache.go`(295L) `backendShim` | ~40 方法同时是写路径、range/count/filter 引擎、watch 事件翻译器、prevKv 缓存、lease 查找、revision 访问器;4 个 cache + 4 个 singleflight.Group;`SetLeaseLookup/SetCountProxy` 注入式 setter 暴露了"职责是后贴上去的"。新人无法判断某处改动的爆炸半径 | 拆成组合的协作者:`rangeShaper`(option/filter/sort/limit)、`watchTranslator`(事件→mvccpb+progress marker)、`prevKvResolver`(_cache.go 已近独立)、`countResolver`;backendShim 退化为薄适配门面 | large |
| A3 | `pkg/backend/txn.go`(783L) | **文件名叫 txn.go 却没有事务**——装着单键 CRUD + watch 事件 ring/overflow 机器(notify/notifyBatch/handleWatchEventOverflow);真正的事务引擎在 txn_apply.go,ring 的另一半(collectStorageWriteEvents 等)在 backend.go。两个无关概念交织,ring 机器无家可归,命名主动误导 | 重命名 txn.go→write.go(单键 CRUD);把 notify/overflow 与 collectStorageWriteEvents 合并进 `eventring.go`,让 ring 的生产者+消费者+溢出同处一室 | medium |
| A4 | `pkg/server/etcd/kv.go`(1333L) `Txn`(111L)+`executeTxnWithPaths`+`tryAtomicGenericTxn`+~24 函数 | 全仓最正确性敏感的 etcd Txn 引擎与一堆 compare/validate 自由函数塞一个文件;更糟的是 `paths []bool` + 共享游标 `pathIndex *int` 在多个函数间穿针,是一个隐式有状态、极易失步的协议 | compare/validate 拆到 `txn_compare.go`/`txn_validate.go`;用显式 iterator 或持有位置的 struct 取代 paths+pathIndex 游标 | large |
| A5 | `pkg/backend/scanner/scanner.go` `worker.run`(396-528,循环 423-505) | 一个 ~130 行深嵌套循环干三件无关的活:list/count 发射、多版本+tombstone 压缩、revision-key GC;`w.compact` 标志门控 4 个分支与读路径交织。**代码自己在第 464 行留了 TODO 承认缺抽象** | 照着 in-code TODO 把压缩建模为 `resultReceiver`/visitor,run() 退化为纯 key-group 迭代器,compaction/GC 成为一种 receiver 实现,从读路径移除 w.compact 条件 | medium |

### 主题 B:重复代码(战役期"就地复制"是主因)

| 项 | 文件+符号 | 为什么痛 | 重构 | 工作量 |
|---|---|---|---|---|
| B1 | `backendshim.go` count 三段式:`Count`(849-911,rev!=0/rev==0 两份)、`exactRangeCountUncached`(695-730)、delta 路径(662-672) | index→leader-proxy→scan 的解析梯子手写三遍,各带近似 #41/#51 注释,**且已悄悄漂移**(Count rev==0 回落 backend.Count,exactRangeCountUncached 回落 List)——正是产生不一致 count 答案的漂移源 | 引入单一 `resolveCount(ctx,key,end,rev)(int64,servedFrom)` 实现梯子一次,各调用点只在响应封装上不同 | medium |
| B2 | `pkg/backend/txn.go` 多处 ring 填充 | 三类填充各写多遍且已漂移:(a) `notify`(669) vs `notifyBatch`(714) 守卫前奏相同,零 revision 分别发 `watch.event.zero_revision.dropped` 与 `watch.event.buffer.invalid`;(b) "已消费但失败的 revision 必须填 invalid 事件否则 collector 永久停滞"这一**安全关键不变量**由 `deleteRangeChunk`(490-506)/`notifyInvalidTxn`(txn_apply 367-389)/单键 notify 三份强制 | notify 构造单元素 slice 委托 notifyBatch;抽 `notifyInvalidRevision(rev,keys/verbs,err)` 让防停滞契约单点存在;顺手统一两个零-revision 指标名 | small–medium |
| B3 | `txn.go` compat 对象写:`createBatchWithMetadata`(175)、`update`(649)、`txn_apply.go putTxnObject`(354) | **正确的 helper `putTxnObject` 已存在**,却只有 txn 路径用它,create/update 仍 inline 手抄;元数据存储约定一改要动三处 | create/update 直接调 putTxnObject,删两份 inline;考虑把 putTxnObject 移出 txn_apply.go | small |
| B4 | `txn.go` `updateOnce`(537-602) vs `deleteOnce`(193-261) | 相同骨架(txnLog→inner→notify→waitCommitted→CAS 失败则 heal-once-retry),已漂移(delete 从 old 建 resp.Kv,update 返回 nil) | 抽出共享的"CAS 失败→heal 一次重试,否则返回最新"尾巴,参数化重试闭包 | medium |
| B5 | `pkg/backend/watch.go` `Watch`(74-152) | "historyWatchEvents→成功则 catchUp+advance+go processEvents;失败则 cancel"序列 + review-#55 错误传播注释,在 empty/low/cached-mid 三分支近乎逐字复制三遍 | 抽 `startFromHistory(...)` 做一次 replay+advance+processEvents+错误交接,三分支各调一次,给 #55 语义单一归属 | medium |
| B6 | `pkg/server/service/etcdproxy/etcd_proxy.go`(356-469)+`disabled.go`+`interface.go` | 八个转发方法在三个文件里三倍存在;共享的 `markForwardError`(死 leader 重置 client)靠每方法手抄,**漏一处会静默失败且 review 看不出**(周围全一样) | 抽泛型 `forward[Req,Resp](ctx,logfields,func(conn)(Resp,error))` 一次性做 readyClient+markForwardError,每方法变一行 | medium |
| B7 | `pkg/metrics/prometheus/prometheus.go` `mustGetGaugeVec/CounterVec/HistogramVec`(164-236) | 三个 ~24 行函数逐字节相同,仅 vec 类型和构造器一行不同;双检锁模式改一次要改三处 | Go 泛型 `getOrCreate[V](mu,map,name,labels,newFn)` 收成一个 | small |
| B8 | `pkg/backend/scanner/scanner.go`:46 + 486-497 | 自定义 `revisionValueLengthWithDeletionFlag=9` 遮蔽已导出的 `coder.RevisionValueLengthWithDeletionFlag`,并手写 revision-value 解析,绕开全仓统一的 `coder.ParseRevision`;wire 格式一改**不会波及 compactor 的 GC 判定** | 删本地常量,改用 coder.ParseRevision,让 revision-value 格式单一权威 | small |
| B9 | `watcherhub.go:93` 与 `etcd/watch.go:83` `storeMaxUint64` | 同一 10 行 CAS helper 逐字复制两包,注释还写着"kept in sync";未来迁 atomic.Uint64 要记两处 | 移到 pkg/util 供两处调用,删副本与注释 | small |
| B10 | `pkg/server/service/etcdproxy/etcd_proxy.go` `waitReady`(504-511, 522-530) | 同一 8 行"not-ready 超时错误"块两个 select 分支各一份 | 抽 `notReadyErr(ctx,lastErr)` | small |

### 主题 C:层泄漏 / 缺失抽象

| 项 | 文件+符号 | 为什么痛 | 重构 | 工作量 |
|---|---|---|---|---|
| C1 | `pkg/storage/interface.go` `BatchWrite` 契约 vs tikv/badger/memkv `batch.go` | 同一 per-key 契约实现出**三种结构**(tikv 延迟带-ctx 闭包 / badger 延迟无-ctx 闭包 / memkv 即时求值+首错闩锁),错误时机与冲突检测形状全不同;**本战役 #44/#45/#65 三个修复全是"让后端 X 的 CAS/错误语义对齐 memkv",各自被迫在不同控制流里重推一遍**,且无共享契约测试防下次漂移 | 抽一套后端无关的表驱动契约测试套件(CAS-on-missing、PutIfNotExist-on-existing、DelCurrent-version-mismatch、commit-失败-uncertain),每个后端都跑过;长期考虑共享 batch 骨架,后端只填 per-op 原语 | medium |
| C2 | `pkg/storage/metrics/store.go` `NewKvStorage/gcStoreWrapper`(29-52) | 可选能力(GC、ExclusiveKvStorage)不在 KvStorage 接口内,embed 不提升,需手工重新暴露;GC 已用额外 wrapper 修好,**但 ExclusiveKvStorage 未暴露**——scanner.go:219 做 `r.store.(storage.ExclusiveKvStorage)`,一旦某后端实现它、经 metrics 包装后类型断言静默失败、优化被关且无编译错无测试。这是 #37 记录过的陷阱只修了一半,且是 O(2^n) 组合风险 | 用单一能力转发 wrapper 检测并重新暴露每个已知可选接口;至少现在补上 ExclusiveKvStorage 直通或加一个"包装后仍满足接口"的测试 | medium |
| C3 | `watcherhub.go:108/114` `newProgressMarker/isProgressMarker` vs `backendshim.go:1035` `isBackendProgressMarker` | "单元素批次且 event.Kv 为 nil = 带内 progress marker"这一跨层协议编码一次、解码两次(两个不同包);marker 表示一改,三处不同步则 watch 静默误分类事件 | 给 marker 单一归属:backend 包导出 `NewProgressMarker/IsProgressMarker`,server/etcd 复用 | small |

*(注:主题 A1 中 lease bind/unbind 被写热路径 inline 触及,本质也是层泄漏,已在 A1 处理。)*

### 主题 D:死代码 & 陈旧/误导注释

| 项 | 文件+符号 | 为什么痛 | 重构 | 工作量 |
|---|---|---|---|---|
| D1 | `pkg/backend/creator/`(naive.go+interface.go)+`backend.go:189,349` | 整个 `creator` 包在 NewBackend 里被构造并存进 `b.creator` 字段,**但 pkg/ 内零读取**;真正的 create 路径在 txn.go 把 naiveCreator 的 CAS 冲突诊断逻辑 inline 重写了一遍。维护者会误以为写流经 Creator——纯误导性"看似承重的遗物" | 删除 creator/ 包与 b.creator 字段(或真把 create 路由过去);现状是纯死重量 | small |
| D2 | `pkg/endpoint/endpoint.go:21` `_ "net/http/pprof"` | 盲导入的 init() 把 pprof 注册到 http.DefaultServeMux,**与同包 #32 设计(pprof.go+EnablePprof 门控,"绝不自动暴露")直接矛盾**;目前只因人人用显式 mux 才惰性无害。维护者信 #32 注释以为 pprof 已门控,任一未来 `ListenAndServe(addr,nil)` 就重开未认证 CPU/heap DoS | 删这行盲导入(pprof handler 已在 pprof.go 显式装配),加一句"按 #32 故意不导入"注释 | small(安全相关) |
| D3 | `kv.go:1028` `evalCompare` + `server.go:56-57` 内嵌 `sync.Mutex` | (1) evalCompare 是 evalCompareGuarded 的薄包装,**零调用**;(2) RPCServer 匿名内嵌 mutex 注释"watcher map mutes",但全包无 Lock/Unlock、RPCServer 也不持 watcher map——却把 Lock/Unlock 提升到 RPCServer,后人可能依赖一个什么都不保护的锁 | 删 evalCompare;删内嵌 mutex 与陈旧注释 | small |
| D4 | 陈旧/误导注释群 | 分散但集中误导:`backendshim.go:440` "compact 对 kube-brain 没必要?"实则 apiserver 在驱动;`kv.go:85-86` 提到不存在的 clientv3 builder 方法;`scanner.go:131` "ban the calling of Count" 上方是活的全扫 Count;`watcherhub.go:154` InfoS 里 `%v` 从不替换成噪音;`memkv/batch.go:85` 死注释行;`config.go:28` ClientTLS 注释说成 peer(与实际 client-side 相反);`txn.go:70-85` 解释已删代码的考古注释;`interface.go:35-49` GC 长篇挂到了 KvStorage 的 godoc 上 | 逐条修正/删除;保留承重的排序/正确性注释,删掉考古 | small(累计 small–medium) |
| D5 | 全仓 45 处 `review #NN`/`FINDING #NN` 标签(txn.go×6, leader.go×5, kv.go×5, backendshim.go×5, lease.go×4, txn_apply.go×4 …) | 解释性散文常有用,但 #NN 指向非团队维护者打不开的 review 线程,读来像"权威可追溯性"却指向虚空,若干是裸标签纯噪音 | 保留 rationale 散文,剥离/去引用 #NN,或替换为 docs/DESIGN.md 锚点这类持久指针 | small–medium |

### 主题 E:不一致惯用法 / 缺失抽象

| 项 | 文件+符号 | 为什么痛 | 重构 | 工作量 |
|---|---|---|---|---|
| E1 | leader/proxy 门控谓词分散 ~15 处(kv.go:53/461, watch.go:161/466, lease.go:447/575/812/847, server.go:114, etcdproxy:140)+ kv.go 五处 follower gate 各自拼错误串 vs lease.go `requireLeaseLeader` | 同一"能否本地服务/转发/拒绝"决策每处用略不同的 IsLeader()/EtcdProxyEnabled() 布尔组合手写,还各带自己的错误消息格式;策略一改要找齐 ~15 个变体并重新推理,且分不清"有意差异"还是 bug | 抽意图命名的 helper(`mustServeLocally(req)`/`shouldProxyRead(req)`/`notLeaderErr(op)`),把 fencing 决策与错误格式收单点;每个写/lease RPC 声明一次策略 | medium |
| E2 | revision 三种拼写/类型(`BackendShim` 接口内 Delete `revision int64` / Update `rev int64` / Compact `revision uint64` / ListByStream `uint64`);全仓 rev/revision/Revision 混用 | 维护者需不断重推某个 revision 是有符号还是无符号、是 etcd 还是 backend 的;etcd wire 是 int64、backend 是 uint64,几乎每个调用点都在转换 | 每层定一个参数名(revision)并显式文档化类型契约:etcd 边界 int64、backend uint64,只在一个接缝转换;至少先让 BackendShim 接口内部一致(全 int64) | medium |
| E3 | 错误包装分裂:backend/storage 用 `github.com/pkg/errors`(20 文件),server 用 `fmt.Errorf`(22 文件),`%w` 仅 2 处真用 | storage 抛出的错误经 server 无 %w 重包后对 errors.Is/As 不透明,导致 `errClass` 只能字符串匹配而非类型检查——跨层错误分类脆弱 | 统一到 stdlib `fmt.Errorf`+`%w`(或全用 pkg/errors),审计补齐 %w,再把 errClass 从字符串嗅探切到类型检查 | medium |
| E4 | ~140 个 metric 名字符串字面量 inline 散落各层(`pkg/metrics/metrics.go` 及全仓) | 无中心常量/注册表:改名或拼错静默创建新序列、无处总览指标面、grep 需知道确切点分串 | 引入 metric 名常量包(typed constants 或小 registry),每次 emit 引用符号,使面板可审计、改名编译期可查 | medium |
| E5 | `pkg/server/config.go` 等 leader/proxy 调参在 4 个手抄 Config struct + 直通 getter 中重复(endpoint.Config→server.Config→leader.Config;getServerConfig/getLeaderConfig/option.Validate) | 加一个 leader/proxy 旋钮要改 ~6 处,漏一层只有运行时行为会暴露——是"漏字段藏身处" | 直接 embed leader.Config 进 server.Config/endpoint.Config 让新字段自动传播,或在 option.go 构造一次按值下传 | medium |
| E6 | leader 身份字符串:`"empty"` 哨兵硬编码 + `strings.Split(",")` 解析散在 5 处(etcd_proxy.go:153/482, revision.go:353, cluster.go:33, maintenance.go:139) | `resourceLock.Describe()` 的 `addr,version` 格式契约泄漏到 5 个独立调用点;哨兵或分隔符一改,就绪检查/proxy 拨号/follower 同步静默非局部地坏 | 引入 leader 包常量 `LeaderUnknown` 与 typed accessor 返回 `(addr,version,ok)`,GetLeaderInfo 空 leader 返回 "" 让调用方只测一个条件 | small–medium |
| E7 | `pkg/storage/memkv/batch.go` `PutIfNotExist/CAS/DelCurrent`(52-134) | 三个产错 op 对"闩锁 b.err 后是否停止"不一致:PutIfNotExist 立即返回,CAS/DelCurrent 设错后仍继续写 cache/opCount(opCount 用作 Conflict Idx),约定含混;第 85 行还有死注释 | 选一个约定(仿 PutIfNotExist 闩锁即返回)应用到 CAS/DelCurrent,删死注释 | small |
| E8 | `pkg/server/etcd/kv.go` Txn shape 检测器 `isCreate/isDelete/...`(634-695) 返回位置化多 bool 元组(如 (int64,[]byte,bool,bool)) | 尾部 bool 语义不同(includeFailureRange vs ok),调用点极易转置;长 dispatch + 无类型多 bool 使新增 txn 形态易读错 | 检测器返回小 typed struct/decoded-op,把 per-branch metric/failedKey/bind 记账收进 helper,每 case 变成 decode→apply | medium |
| E9 | `pkg/server/etcd/kv.go` `compareSingleKey`(1061) vs `compareKeyValue`(1121) | 两份 ~30 行 switch 编码相同 5 目标 compare 真值表,仅 nil-kv/LEASE 来源不同;新增/修正 compare 目标要镜像两处,易只落一处 | 合成一个 `compare(cmp,kv)`,把两处 nil 差异折进显式分支,range 路径循环它、point 路径包装它 | medium |
| E10 | 小不一致:`election.go` Create(158)/Update(195) 用 `context.Background()` 无超时 vs getTso(132) 走 genContext;`brain/write.go` Compact(112-120) 漏 deadline 预检+WithTimeout;`badger/iter.go inRange()`(50-63)是谓词却 `counter++` 有副作用、三后端迭代器各造一套 border/limit 机制 | 各自个别无害,但"跨切面一改要逐方法手动"且 Compact/election 的不一致看着像疏漏而非决定 | election 用 genContext 统一;write 把 deadline+timeout+metric 收进 wrapper/interceptor 并显式决定 Compact;badger inRange 改名或拆出计数、长期收敛迭代器边界检查 | small 各项 |

---

## 三、推荐优先处理 Top 5(按 维护痛点 ÷ 工作量 排序)

1. **删除 `pkg/endpoint/endpoint.go:21` 的 `_ "net/http/pprof"` 盲导入(D2)** — 一行改动,消除与 #32 设计直接矛盾的**安全脚枪**(未认证 pprof DoS 重开风险)。杠杆最高:成本≈0,收益是关掉一个潜在安全回归 + 一处误导。

2. **删除 `pkg/backend/creator/` 死包与 `b.creator` 字段(D1)** — 小工作量,消除全仓最误导的"看似承重实则零调用"遗物;任何学习写路径的人都会被它带偏,删掉直接止损。

3. **让 create/update 复用已存在的 `putTxnObject`,并合并 notify/notifyBatch/notifyInvalidRevision(B3+B2)** — 小到中工作量,收拢**安全关键**的元数据存储约定与 collector 防停滞不变量到单点;当前是"改元数据约定要动三处、防停滞契约有三份副本",正是易出静默 bug 的地方。

4. **抽 `resolveCount` 梯子 helper 收拢 count 三段式(B1)** — 中等工作量,但直击**规模北极星**下最要命的正确性风险:index→proxy→scan 解析已在三处漂移、能产生不一致 count 答案;收单点后 fallback 策略一改只动一处。

5. **抽 BatchWrite 后端契约测试套件(C1)** — 中等工作量,但它是本战役 #44/#45/#65 三个修复的共同根因(每次都要在不同后端控制流里重推 CAS/错误语义);一套表驱动契约测试让下一次漂移在 CI 失败,而非在 3300 万规模的生产里被发现。

> **战略提示(不入 Top5,但需排期):** 三大神对象(A1 `RPCServer`+lease、A2 `backendShim`、A4 `kv.go` Txn 引擎)是最高的**绝对**维护痛点,但都是 large 工作量、杠杆比不上上述快赢。建议在快赢清理后,单独立项按"抽 leaseManager""拆 backendShim 协作者""拆 txn_compare/validate + 去掉 paths 游标"三步渐进推进,每步保持 `-race` 与规模回归绿。