# 多租户 GC 部署说明(共享 PD/TiKV)

本页说明在**一套 PD/TiKV 上部署多个 KubeBrain 租户**时,垃圾回收(GC safepoint)的语义、必须遵守的配置约束,以及什么时候必须改用独立 cell。

> 前置阅读:`--keyspace` 的数据键隔离见 [dbaas_compatibility_plan_cn.md](./dbaas_compatibility_plan_cn.md) / #76。本页只讲 GC。

## TL;DR

- 共库多租户**每个租户必须配不同的 `--keyspace`**。配了 keyspace,GC 隔离**自动生效**,无需额外开关。
- keyspace 隔离了**数据键**,本次修复(#76)补上了隔离**GC 保留策略**:租户之间不会再误删对方还需要的 MVCC 版本。
- 仍有一条硬边界:**TiKV 全集群只有一个物理 GC safepoint**。共库租户的物理回收线 = 所有租户里最保守那个。要每租户**独立的物理回收节奏**,只能分到**独立 cell(每租户一套 PD/TiKV)**。

---

## 背景:GC safepoint 是怎么工作的

KubeBrain 把自己的 MVCC 编进 key 里,每次快照读都用当前时间戳,所以引擎层的历史版本对它纯属垃圾。但在裸 PD+TiKV 上,**没有 TiDB 的 `gc_worker` 去推进集群 GC safepoint**,每次 CAS 覆盖留下的旧版本会无限堆积,读延迟单调恶化(现场实测:`gc_safe_point=0` 时单点 GET 100ms+,加了 GC 驱动后回到 ~9ms)。

于是 KubeBrain 的 leader 周期性地扮演 `gc_worker` 角色:

1. 取 PD TSO 算出 `target = now - --storage-gc-lifetime`;
2. 用一个 **service 名**向 PD 注册"别 GC 到我这条线以下"(`UpdateServiceGCSafePoint`),拿回**所有 service 的 min**;
3. 把集群 safepoint 推到 `min(target, 其他 service 的 min)`(clamp),TiKV 的 compaction filter 随后回收 safepoint 以下的版本。

关键点:**集群 GC safepoint 是全集群单值**;service safepoint 只是各方注册的"地板",集群实际回收线 = 所有地板的 min。

---

## 问题:共用一条 `gc_worker` 记录(#76 修复前)

修复前,**每个租户都用同一个保留 service 名 `gc_worker`** 注册。后果:

- last-writer-wins:后写的租户覆盖前一个,`UpdateServiceGCSafePoint` 返回的 min **看不到别的租户的地板**。
- 一个配了短保留(比如 1h)的激进租户,会把集群 safepoint 推到 1h 前,**回收掉另一个需要 7d 保留的租户还在用的 MVCC 版本**。

也就是:keyspace 把数据键隔离干净了,但 GC 策略没隔离。

---

## 修复:每 keyspace 一条 service safepoint + 读全表取 min

> ⚠️ 第一版修复("每 keyspace 一条独立 service safepoint,靠 `UpdateServiceGCSafePoint` 返回的 min 来 clamp")经真 PD **实测无效**:PD 会把它自留的 `gc_worker` 占位记录(值=0、无限 TTL)算进那个 min,于是在 KubeBrain 独占集群上返回的 min 恒为 0,clamp 从不生效,激进租户照样推高**唯一的**集群 safepoint、踩掉保守租户的版本。下面是修好的 **Option A**。

**两步机制**:

1. **每租户注册自己的 per-keyspace service safepoint**(名字按 keyspace 派生),让自己的保留地板对所有共库租户**可见**:

   | keyspace | PD service 名 | TTL | 说明 |
   |---|---|---|---|
   | `""`(默认/单租户) | `gc_worker` | 无限 | 保留 `gc_worker` 特殊角色,**行为与修复前逐字节一致** |
   | 命名(如 `tenant-a`) | `kubebrain-ks-tenant-a` | 有限(30min) | 每租户独立一条 |

2. **推进集群 safepoint 前,读 PD 的 service-safepoint 全表**(HTTP `GET /pd/api/v1/gc/safepoint`),对**真实地板**(`safe_point > 0` 且未过期)取 min,把这个 min 作为要推进的目标。这一步**绕开** `gc_worker=0` 占位,也**跳过**已过期(退役)租户。

效果:

- **保守租户不被踩**:唯一的集群 safepoint 落在**最保守的活租户**那条地板 → 任何租户的激进 lifetime 都删不掉别人还需要的版本。**真 PD/TiKV 实测通过**(见 [multitenant_gc_lab_validation_cn.md](./multitenant_gc_lab_validation_cn.md))。
- **单租户 / 默认 keyspace 零变化**:全表里只有自己那条地板 → 推进目标 = 自己的 target,与修复前逐字节一致。
- **死租户会自愈**:命名记录带**有限 TTL = 3 × 最大续租间隔(10min)= 30min**。退役租户的地板在最后一次续租后 ~30min 过期,被"取 min"时跳过,不再**永久顶住全 cell 的回收**。默认 `gc_worker` 用无限 TTL 是安全的(一套集群只有一个这种角色)。
- **读全表失败时**:降级为单 service min(那一轮跨租户保护失效),并打 warning 日志——持续失败可见,不静默。

续租由 **leader** 每 `min(--storage-gc-lifetime, 10min)` 做一次;30min TTL 给约 3 个续租周期裕量,扛得过一次 leader 故障切换(秒级)或一轮被拖慢的 GC。

---

## 部署约束(务必遵守)

### 1. 每租户配不同 `--keyspace`

```
# 租户 A 的所有 KubeBrain 副本
--keyspace=tenant-a  --pd-addrs=<共享PD>  --storage-gc-lifetime=10m

# 租户 B 的所有 KubeBrain 副本
--keyspace=tenant-b  --pd-addrs=<共享PD>  --storage-gc-lifetime=10m
```

- **同一租户的所有副本 keyspace 必须一致**(否则副本间数据不互认)。
- **不要让多个租户都留空 `--keyspace`**:空 keyspace = 传统单租户空间,大家会撞回同一条 `gc_worker`,退化成修复前的行为。共库多租户**必须**每个都命名。

### 2. `--storage-gc-lifetime` 至少要有一个租户 > 0

- 该 flag 控制 MVCC 保留时长,`0` = 关闭 GC 驱动。
- 裸 PD+TiKV 上,如果**所有**租户都关掉 GC(且没有 TiDB 在推),版本会无限堆积、读恶化。共库里**至少要有 GC 驱动在跑**。
- 各租户可以配**不同**的 lifetime——Option A 保证不同 lifetime 下**不互相踩**(保守租户不被删)。但注意下面那条物理边界。

### 3. 记住物理回收边界

即使每租户配了不同 lifetime,**TiKV 实际物理回收到的线 = 所有租户 min(now - lifetime_i)**。

- 例:A 配 1h、B 配 7d,则集群 safepoint 停在 7d 前。A 那些 1h~7d 之间的墓碑/旧版本**照样占着空间**,得等到 B 的 7d 线推过去才放。
- 结论:**Option A 给的是"配置隔离 / 正确性(不误删)",不是"物理回收隔离"**。要各租户独立回收节奏,只能上 cell。

### 4. 两个已知边界(设计使然)

- **冷启动竞态**:集群 safepoint 单调只增。若某激进租户在某保守租户**从未注册过地板之前**就先推进了 safepoint,则那一下会卡住(单调回不来),保守租户在该窗口内的历史会被回收;之后在**约一个保守-lifetime 内自愈**。稳态(保守租户已在跑,激进租户后加入)**无此问题**——实测覆盖的就是稳态。
- **每租户各推一次 GC**:每个租户的 leader 都会调 `KVStore.GC`(全键空间 resolve-locks)。租户数很多时是重复开销(正确性无碍,是性能项)。

---

## 什么时候必须上独立 cell

如果出现下面任一诉求,keyspace 隔离**不够**,必须把该租户放进**独立 cell(自己一套 PD/TiKV)**:

- 某租户要**独立、激进的物理回收节奏**(不想被别的租户的长保留顶住空间);
- 要**爆炸半径隔离**(一个租户把 PD/region 打爆不影响别人);
- 单套 PD/TiKV 已接近容量天花板(PD TSO 吞吐 / region 调度上限 / 每租户全键空间 GC 扫描的聚合背压)。

cell = 再部署一整套独立 PD+TiKV,单套装一批租户,靠"加 cell"而不是"往一套里塞更多租户"来水平扩容。租户→cell 的路由/编排由上层 DBaaS 控制面(`hack/production` 的 operation 面)负责。

---

## 排障

| 现象 | 可能原因 | 排查 |
|---|---|---|
| 某租户空间迟迟不回收 | 被同 cell 另一个长保留(或卡死)租户的地板顶住 | 查 PD 各 service safepoint:`pd-ctl service-gc-safepoint`,看 min 是哪条 service |
| 死租户退役后 cell 回收停滞 | 该租户 `kubebrain-ks-*` 地板尚未过 TTL | 等 ~30min 自动过期;或确认它确实没有残留 leader 还在续租 |
| 全 cell `gc_safe_point` 冻结 | 可能有租户 leader 卡在续租、或某 service 地板异常低 | 查各租户 leader `storage.gc.safepoint` 指标与 PD service safepoint 列表 |
| 命名租户仍互相误删版本 | 有租户误配成空 `--keyspace`,撞回 `gc_worker` | 确认每个共库租户都配了唯一命名 keyspace |

相关指标(leader 上报):`storage.gc.safepoint`(推进后的集群 safepoint)、`storage.gc.err`(推进失败计数)。
