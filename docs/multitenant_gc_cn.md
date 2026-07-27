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

## 修复:每 keyspace 一条 service safepoint

现在 service 名按 keyspace 派生:

| keyspace | PD service 名 | TTL | 说明 |
|---|---|---|---|
| `""`(默认/单租户) | `gc_worker` | 无限 | 保留 `gc_worker` 特殊角色,**行为与修复前逐字节一致** |
| 命名(如 `tenant-a`) | `kubebrain-ks-tenant-a` | 有限(30min) | 每租户独立一条 |

效果:

- **各租户地板都进 min**:`UpdateServiceGCSafePoint` 返回的 min 现在横跨所有共库租户,clamp 会保护每个租户的保留线,激进租户**再也删不掉别人的版本**。
- **死租户会自愈**:命名记录带**有限 TTL = 3 × 最大续租间隔(10min)= 30min**。一个下线/退役的租户,其地板在最后一次续租后 ~30min 过期,不会**永久顶住全 cell 的回收**。这正是命名记录**不能**用无限 TTL 的原因(默认 `gc_worker` 用无限 TTL 是安全的,因为一套集群只有一个这种角色)。

续租由 **leader** 每 `min(--storage-gc-lifetime, 10min)` 做一次;30min 的 TTL 给了约 3 个续租周期的裕量,足以扛过一次 leader 故障切换(秒级)或一轮被拖慢的 GC。

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
- 各租户可以配**不同**的 lifetime——这正是本次修复解锁的能力(修复前不同 lifetime 会互相踩)。但注意下面那条物理边界。

### 3. 记住物理回收边界

即使每租户配了不同 lifetime,**TiKV 实际物理回收到的线 = 所有租户 min(now - lifetime_i)**。

- 例:A 配 1h、B 配 7d,则集群 safepoint 停在 7d 前。A 那些 1h~7d 之间的墓碑/旧版本**照样占着空间**,得等到 B 的 7d 线推过去才放。
- 结论:**per-keyspace safepoint 给的是"配置隔离/正确性",不是"物理回收隔离"**。

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
