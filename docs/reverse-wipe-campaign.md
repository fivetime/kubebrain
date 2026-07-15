# 反向大清除战役:1740 万对象删除与 TiKV/PD 物理干净度终验(2026-07-15,#74)

**目标**:把 KWOK 超大规模实验的全部数据(~1660 万 k8s 对象)删干净,验证 KubeBrain→TiKV/PD 的五级回收链路在**极端墓碑积压**下的完成性,并顺带压测 k8s 原生删除路径的规模行为。

**方法**:混合路径——全量 LIST 超过 2GiB 硬墙的资源类型(deployments/configmaps)与 ns 级联不可行的部分走 bigstream 直连 etcd 协议删除;nodes/namespaces 走 k8s API(loadgen),级联交给 kcm。

## 删除总账(全程 0 失败)

| 阶段 | 内容 | 数量 | 耗时/速率 |
|---|---|---|---|
| A | deployments + configmaps(bigstream) | 490 万 + 600 万 | 51min,3189-4071/s |
| B1 | replicasets + pods(bigstream,先断补建源) | 30.4 万 + 28.2 万 | 8.5min |
| B2 | nodes + namespaces(**k8s API**,loadgen) | 10 万 + 100 万 | 1.5min + 11.6min,1055-1433/s |
| B3 | ns-* 的 SA/endpointslices/events/ns 兜底(bigstream) | 199 万 + 100 万 + 0.2 万 + 99.4 万 | ~16min |
| C | roles(census 揪出的漏网)+ 孤儿复生 cm + kwok 事件 | 99.3 万 + 5 万 + 1.7 万 | ~5min |
| **合计** | | **~1738 万 keys** | **~1.5h 纯删除** |

## 终验(五级链路全绿)

| 层级 | 删除前基线 | 终态 |
|---|---|---|
| 逻辑键空间(/registry/ 全前缀 count) | 16,565,985 | **310(纯系统对象)** |
| KubeBrain 物理 GC(`compact` 行计数) | — | **53,798,012 行清除**,done_rev 追平 current |
| count index | 16.68M | **~416**(轮末 `countIndex.Compact` 一次性剪墓碑) |
| PD gc_safe_point | 推进中 | 持续推进(storage-gc driver 每 10min) |
| TiKV kv 引擎(`tikv_engine_size_bytes{db="kv"}`) | 2.3-2.5 GB/副本 | **65-87 MB/副本**(手动 compact 后进一步收敛) |
| PD region 数 | 85 | **20**(空 region 自动 merge) |

## 极端墓碑积压下的物理 GC(#66 的终极现场)

删除高峰期(k3s 停,无人发 Compact)累积 ~1200 万墓碑后恢复 compaction:

- 单轮全量扫描背着 1700 万 keys 的墓碑 + TiKV 层未清的 MVCC 版本(读放大),**约 75 分钟完成**,期间进程 ~4 核忙、内存峰值 4.2GB,轮末一次性释放;
- `backend_compact_incremental_fallback` 4→5:**>10 万 touched keys 正确触发增量 GC 回退全扫**(设计验证 ✓);
- 墓碑沼泽自排水:GC 追平后同形态操作延迟恢复正常;
- ⚠️ **可观测性缺口(改进项)**:行删除计数与 done_rev 都在轮末/分区边界才更新,超长轮在指标上呈"冻结"状,极易被误判为死锁(本次差点无谓重启 leader)。建议:轮内心跳日志(每 N 分区)+ 进行中分区计数 gauge。

## 沿途撞出的四堵墙(均已入 survival-stage0-cn.md)

1. **2GiB gRPC 硬墙**:≤1.36 的全量 LIST 单响应超 int32 → 冷启动永久失败(490 万 deploy=2.9GB 实测);真 etcd 同中招;1.37 RangeStream 是唯一出路——**可用性问题,不是性能问题**;
2. **kcm 默认 `kube-api-qps=20`**:100 万 ns 级联 0.9 ns/s ≈ 12 天;
3. **ns 级联固有成本 ~243 API 调用/ns**:QPS 拉满(实测 kcm 835 req/s,KubeBrain 稳定承接)也只有 3.4 ns/s ≈ 80h/百万——百万 ns 规模按类型删,别走级联;
4. **k3s wrangler node finalizer**:10 万 node DELETE 秒回成功,对象按 ~70/s 逐个消失。

另:孤儿 RS 在被 GC 收走前仍补建 pod(GC ~20/s 跑不过 replicaset 控制器)——删除顺序必须 Deployment/RS 先于 pod。

## 教训(操作侧)

- 终验普查必须做**键空间区间二分计数**,不能只点名已知类型(roles 99.3 万差点漏网);
- etcdctl `--write-out=fields` 首行是 `"ClusterID" : 0`,粗暴 `grep -oE '[0-9]+' | head -1` 会拿到 0;
- 判断磁盘占用看 `tikv_engine_size_bytes{db="kv"}`;终态 `du db/` 3-3.7G 看似未收敛,实际内容是 **5 个 SST(=65-87MB 真实数据)+ 3.6G 删除高峰期的 RocksDB WAL(.log,随 memtable flush 自行回收)**;raft-engine 11G 同为引擎自管日志空间——`du` 陷阱家族(#69 的 placeholder/raft 之外再添 WAL 一员)。
