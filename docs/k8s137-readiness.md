# KubeBrain × Kubernetes 1.37 就绪度报告与运维指引

日期:2026-07-14 · 对照基线:k8s 1.37 master(vendor etcd api/client v3.7.0)vs KubeBrain main
审计方式:多 agent 双侧源码契约对照(k8s 1.37 实际发什么/期待什么 ⇄ KubeBrain 实际实现),每条发现经对抗验证。

## 总判断

**有条件 READY**:未发现任何默认路径破坏(零 GAP);审计发现的 3 项 P1 + 5 项 P2 风险已全部修复(见下文)。

## 1.37 与存储层相关的关键变化

| 变化 | 对 KubeBrain 的影响 |
|---|---|
| **EtcdRangeStream(Beta,默认开)**:watch-cache 初始化改走一次 `KV.RangeStream` 流式 RPC 替代分页 Range | KubeBrain 原生实现已就绪(#65);apiserver 只发 prefix range(无 Limit/Rev/Sort/Filter),完全在支持子集内;形状不支持时 Unimplemented → apiserver 自动回退分页并 10min 重探。⭐**这不只是性能项而是可用性项**:≤1.36 的 watch-cache 初始化是不分页全量 Range,单资源类型响应 >2GiB(gRPC int32 上限)时冷启动永久失败——现场坐实:490 万 deployment=2.9GB,k3s 1.36 每 40s 重试死循环 18h+;真 etcd 同样中招(`v3rpc/grpc.go` MaxSendMsgSize=MaxInt32)。千万级对象必须 1.37+RangeStream,详见 survival-stage0-cn.md 的"2GiB 硬墙" |
| **ConsistentListFromCache 锁死 GA**:一致读阻塞在 watch progress 上,100ms 轮询、3s 超时后回退全量 LIST | KubeBrain 支持 in-band per-watch ProgressRequest;本轮加固:RequestProgress 触发即时 marker 扇出 + `--watch-progress-notify-interval` 校验(必须 < 2.5s) |
| **kubeadm 外部 etcd 版本下限抬至 3.5.24-0** | KubeBrain 自报 etcd 3.7.0(gRPC Status + HTTP /version),通过 |
| **kubeadm 新增 `ExternalEtcd.HTTPEndpoints`** | `/version` 现同时注册在 client 口与 info 口,任一口都可承接预检 |
| **StorageVersionMigration GA** | 见下方 runbook |
| 新资源前缀(PodGroup/resource.k8s.io/v1 等)、CBOR 存储编码、WatchListCompression | 对 KubeBrain 透明(key/value 均为不透明字节;value envelope magic 0x00 与 CBOR 前缀无碰撞) |

## 本轮修复清单(审计 P1/P2)

- **P1** RequestProgress 即时扇出(`WatcherHub.KickProgress`,FIFO marker 不越序)+ `--watch-progress-notify-interval` 上限校验(< 2.5s,fail loudly)——封死一致读的 3s 全量 LIST 回退悬崖。
- **P1** `--compatible-with-etcd` 默认改 **true**——原默认 false 时多副本部署漏配该 flag 会让 follower 拒写,clientv3 对 mutable RPC 不重试 → ~2/3 写持续失败而读自愈。非 etcd(brain-client)消费者显式关闭。
- **P1** RangeStream 内部通道缓冲 1000→8(带 value 路径)——gate 默认开后这是 apiserver 冷启动默认路径,深缓冲=每流 30 万个带 value KV 的内存尖峰;浅缓冲让 gRPC 流控背压穿透到扫描。keysOnly(count-index rebuild)保留深缓冲。
- **P2** 分区 worker 已流出块后禁止重试(`retriable()`)——重扫会重发 key 违反 disjoint-chunks 契约;现改为流以错误终止,客户端干净 relist。
- **P2** RangeStream 收到 `Limit>0` 返回 Unimplemented——分区并行扫描无法全局截断,静默忽略会给"无错误的错答案"。
- **P2** info 口注册 `GET /version`。
- **P2** MemberList 的 ClientURLs 按 client 口 + 实际 TLS scheme 构造(原 `http://host:peerPort` 会把 AutoSync 客户端引向不服务 KV 的明文端口)。

## 1.37.0-alpha.3 现场实测(冷启动 A/B)

kube-apiserver v1.37.0-alpha.3 裸二进制打 3 副本 KubeBrain(~1650 万对象),A=修复前镜像(RangeStream 缓冲 1000,progress interval=5s),B=修复后(缓冲 8 + RequestProgress 即时扇出,interval=1s):

| 指标 | A(修复前) | B(修复后) |
|---|---|---|
| RangeStream 流(基线 0,1.36 不调用) | ~62 条,零回退 | 58 OK + 1 正确的 Unavailable |
| 平均流时长 | 10.45s | **4.56s(2.3×)** |
| apiserver init 到 RSS 平台 | ~34 min | **~12 min(2.8×)** |
| 慢分页 LIST(>500ms trace) | 10250 | **531(19×降)** |
| apiserver "Too large resource version"(watch-cache 追不上) | 5723 | **3** |
| KubeBrain leader RSS | 全程 ~7G 平 | 全程 ~7G 平 |

结论:RangeStream 收网现场坐实;progress 即时扇出 + interval 护栏是最大现实收益。注意 **alpha.3 的 `EtcdRangeStream` default=false**(master 才翻 true),alpha 实测须显式开 gate。

**大对象缓冲实验**(20 万×10KB=2GB,同 20ms/块慢消费直连流):缓冲 1000 的服务端 RSS 尖峰 **+1.30GB**,缓冲 8 仅 **+0.37GB**(gRPC 窗口+GC 滞后)——3.5× 差,消费者更慢/数据更大时旧行为线性恶化、新行为恒定。

实测另抓出两个修复:RangeStream 预流失败裸 error 以 Unknown 面世(现按 etcd `togRPCError` 结构整形:ctx 错误透传、status 透传、其余瞬时类包 Unavailable);client 主动取消曾计入 `backend.list.by.stream.failed`(现独立 `canceled` 计数)。

## 增量物理 GC

物理版本-GC 原先每轮 compaction 全 keyspace 扫描(千万级下每 5 分钟蹚一遍全库)。现在每轮先尝试**增量**:从 event log 取 (上轮基线, 本轮 watermark] 被写过的 key,仅对这些 key 的版本区间跑同一套 GC 规则。回退到全扫的条件:进程内尚无完成基线 / 上次全扫失败 / event log 窗口不足 / 触碰 key 超 10 万(如 SVM 重写风暴——全扫本来更便宜)/ 增量出错 / 每 48 轮强制全扫兜底(约 4 小时)。稳态下每轮从 O(全库行数) 变为 O(近期写入 key 数)。观测:`backend.compact.incremental`(轮数)、`backend.compact.incremental.keys`(每轮工作集)、`backend.compact.incremental.fallback/err`(回退)。

## 运维 runbook

### StorageVersionMigration(1.37 GA)在超大规模下

SVM 仅在**显式创建 SVM CR** 时触发,但 KCM 给它的 client 特批 QPS×20/Burst×100(默认 400 QPS/3000 burst)。对 1000 万对象的资源做迁移 ≈ 连续 7 小时写洪水、revision +10M、临时 10M 陈旧版本。迁移前:

1. **确认 TiKV GC safepoint 推进在位**:`storage_gc_safepoint` 指标持续推进(KubeBrain `--storage-gc-lifetime` 默认 10m;裸 PD+TiKV 无 TiDB 时必须依赖它)。
2. **监控 compaction 追平**:`compact_physical_done_rev` 与 `leader_revision` 的差值不应持续拉大;`compact` 计数应随迁移持续增长。
3. 预期迁移窗口内 LIST/count 延迟有可观测退化(MVCC 版本堆积),迁移完成 + 一轮 compaction 后恢复。
4. 避免同时迁移多个大资源;retriable 错误会导致该资源整体重跑。

### clientv3 AutoSync

仅在所有副本都配置相同的 `--initial-cluster=name=peerURL,...` 后开启 clientv3 `AutoSyncInterval`;此时 MemberList 返回 DBaaS 控制面注入的完整 KubeBrain 服务副本集合。`deploy/production/kubebrain*.yaml` 已使用 StatefulSet、headless peer Service 和稳定 Pod DNS 配置完整三成员集合。自定义 DBaaS 控制面必须生成等价配置；未配置时仍只回退返回自身与 leader,Sync 会缩小客户端端点集合。

所有需要 follower revision fence 的 RPC（Range/RangeStream、Watch 控制请求、Status/Hash、MemberList，以及相关写路径）都会执行 read barrier。leader 切换或 revision 同步失败时，普通内部错误会整形为 gRPC `Unavailable`，使 clientv3 按可重试故障处理；已有 gRPC 状态以及 `Canceled`/`DeadlineExceeded` 保持不变。

RangeStream 的 identity metadata 位于 `RangeStreamResponse.range_response.header`。KubeBrain 与 etcd 一样只在 terminal chunk 返回该 header，并填充非零 ClusterId、MemberId、RaftTerm 与 pinned revision；中间数据块不携带 header。

### 其他

- `etcd_db_total_size_in_bytes`(DbSize)为 1 字节兼容哨兵(TiKV 容量语义不同)。容量观测走 TiKV/PD 指标带外抓取;依赖 DbSize 的告警需改造。
- 北极星规模(千万级)建议开启 apiserver 的 `ConsistentListFromCacheSkipTimeoutFallback` gate(1.37 Alpha):progress 超时改返 429 而非穿透存储全量 LIST,对存储纯减压。
- `etcdctl snapshot save` 不支持(Unimplemented);备份走 TiKV 生态(BR)。
