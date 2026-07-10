# KubeBrain 千万级实战战役终版战报

**战役周期**:2026-07-07 ~ 2026-07-10 · **被测对象**:KubeBrain(etcd-v3 兼容 shim over TiKV)@ main
**环境**:3 机(80C/300G、72C/269G、104C/362G)+ KWOK 模拟 10 万 node + 真实 kube-apiserver/kcm/scheduler(k8s 1.36 补丁版)

## 一、最终版图

| 对象 | 数量 |
|---|---|
| **keyspace 总量** | **33,431,344 keys**(TiKV 占用 149G/300G) |
| deployments | 8,838,096 |
| replicasets | 8,838,096(与 deploy 精确 1:1) |
| pods | 8,838,096 · 全部 Running |
| secrets | 5,000,002 |
| services | 1,160,000(headless) |
| namespaces | 500,000 |
| nodes | 100,001(KWOK 6 分片) |

## 二、KubeBrain 十大修复(全部合入 main)

| commit | 修复 | 实测效果 |
|---|---|---|
| a143a89 | List:per-key ctx 分配 + 每页 O(range) count → 滚动增量 | 890→25ms/页(35×) |
| 49505a5 | per-watch RequestProgress(替代 stream-min) | cacher 429 死循环解除 |
| d8542a5 | 全局扫描并发 cap 24 + 单分区 bypass | 2-key List 12.6s→正常 |
| 2f34e9b | compact per-key V0 日志降级(journald 背压) | rebuild 3千/分→23万/分 |
| 448ebc8 | **count-index rebuild 一次并行全扫**(替代分页) | 80min 必死→**3343 万 keys/1m49s** |
| 1261cf2 | **PrevKv 事件流自答**(prev-hint + 并行预取) | watch 流 60 ev/s 天花板拆除 |
| 0f27dc0 | **写队头窗口收缩**(update Get 前移 + create 重试换 rev) | CAS 失败 35%→0、commit-wait 552→162ms、裸写 p50 739→285ms |
| 6921dae | **事件日志 keyspace**(9B/条同事务、rev 区间回放) | 历史回放 O(keyspace)→O(窗口事件);生产 58 次回放实证 |
| 53c06e1 + 0492c84 | **follower count 代理 leader index** + proxy 拨号端口修复 | follower count 60s 超时→热 1.0s;顺带修复历史读代理从未工作的隐藏 bug |
| 5c72069 | gRPC keepalive enforcement 对齐 etcd(MinTime 5s) | GoAway too_many_pings 踢空闲 watch 根治 |

## 三、k8s fork 三补丁(fivetime/kubernetes,上游 PR 素材)

1. `79605df` services repair 首轮失败指数退避快速重试(替代等 3 分钟整周期;并修复 repairip 首轮失败永久放弃)+ 回归测试
2. `b280c43` repair post-start 死线 1min→15min(百万 Service 规模)
3. repairLoopInterval 3min→30min(每轮全量 List 百万 services 在千万 keyspace 下持续挤占写路径)

## 四、十三堵 k8s 规模墙(定量实测)

1. **repair hook 三重死锁**(60s 死线 × 3min 重试 × 429 自拒)→ fork 补丁
2. **cacher init 单线程 decode ~500µs/对象**(50 万 ns=4m16s;874 万 pods≈70min)→ 重启不可用时长的硬约束
3. informer 全量墙(300 万 ns 不可行 → 定格 50 万)
4. admission 的 informer 依赖(未 ready 拒一切写;实际报错源 NodeDeclaredFeatureValidator/PodTopologyLabels)→ 14 项禁用
5. ClusterIP 全局串行分配(4/s)→ headless(720/s)
6. **大 List 跑不过 compaction**,两种形态:①continue token 快照被 compact ②List 时长 > compact 间隔时,完成即 watch 起点 410,整轮白做无限重来 → **compaction 间隔必须 > List 一轮最坏时长**(终值 6h)
7. cacher watch 积压死锁(init 期间事件积压 → 消化 60/s < 写速 → informer 永不 ready)→ 零写入窗口重启
8. controller 重启重同步税(kcm 每次重启:informer 直通重拉 1-2.5h + deployment status 全量重刷 ~2h)
9. `--etcd-count-metric-poll-period=0` 必设(count 轮询在 index 不可用时全量扫)
10. **`SizeBasedListCostEstimate=false` 必设**(1.34+ Beta 默认开:每资源每分钟全量 KeysOnly List = 千万级持续全库扫描,TiKV gRPC 前端饱和,写 p99 189ms→关后 45ms)
11. **kcm 必须去掉 `--service-account-private-key-file`**(TokensController 绕过 --controllers 过滤,500 万 secrets 每 4 分钟一轮 List)
12. **千万级单资源三关**:watch-cache 关(cacher ready 反而让 consistent-list 掉到 0.5 页/s)+ WatchListClient 关(1.35+ 默认 true;883 万对象 WATCHLIST 流 20min 不完成;且影响 apiserver 自身 loopback informer 的 sync 全量 Range)+ 直通分页(KubeBrain 25-65ms/页)。WatchList 对中小资源仍是正解(17 万 RS 8s/流)
13. **apiserver watch-cache 内存墙 = 单集群 k8s 的终极规模墙**:cache 全量驻留(50 万 pods=54.7G → 500 万 ≈ 300G,整机 OOM 死机 ×2);init 膨胀 2-6 倍(稳态 40G 的 init 冲 239G);kcm informer store 168G、scheduler 105G(883 万三巨头)——三台大内存机无法全组件同跑,时分复用/分片是唯一出路

## 五、稳态基线(3,343 万 keys,全组件运行)

| 指标 | 数值 |
|---|---|
| 裸写 PUT p50(leader) | 177ms |
| 单键读 p50/p99(经 pods-shard cache) | 41ms / 81ms |
| count(883 万对象,index walk) | 1.6-2.3s |
| List 直通分页 | p50 177ms/页 |
| 直灌写吞吐(实测峰值) | **3,818 写/s 零失败**(独占);双引擎 4,900 写/s |
| per-node 索引查询(shard cache) | 341ms(cache 关闭时 60s 超时,176×) |

## 六、DR 实战(#46,非计划实弹)

leader 机在 3,400 写/s 洪流下整机 OOM 死机:
- **follower 秒级当选**(18:35:47)→ event log 水位推进 +1s → **count-index 重建 33,429,458 keys 仅 1m49s**
- 对照 50 万时代(74s/72 万 keys):keyspace 46×,rebuild 时长仅 1.5×
- GC/leader 职责无缝接管,**数据零丢失**;原 leader 上电自动 rejoin follower 不抢占

运维铁律:etcd client 全双端点;fs.nr_open 持久化;**KubeBrain leader 不与 TiKV 同机**(同机时 collector 爬行 70/s、写全 3s backstop,迁离立愈)。

## 七、发布风暴(#48)

10,000 deployment 滚动更新(template patch 触发真实滚动:新 RS + 新 pod + 调度 + 缩容旧代):
- **点火:10,000 patch / 71 秒(141/s,p50=122ms)零失败** —— KubeBrain 写路径全程健康
- **消化:kcm sync 后一个窗口内产出 8,848 个新 RS(88%+)**,滚动本身对存储毫无压力
- 端到端时长被三件消费端事故支配(均非存储):①墙 6 第二形态(List 2.5h/轮 > 2h compaction → 整轮重来,终解 6h)②墙 13 再证(shard+kcm+scheduler 内存互杀 → 时分复用)③墙 8(kcm 重启重同步税)
- 结论:**滚动风暴的瓶颈在 k8s 控制器的重同步经济学,存储层是旁观者**

## 八、架构结论:通往生产形态

实测铁律:**存储层(KubeBrain/TiKV)余量巨大;apiserver 内存是单集群的终极墙。**

生产形态 = 单 KubeBrain + 多原生 apiserver cache 分片:
- 手工分片已验证:pods 专属 apiserver(独立 cache 206G)与主 apiserver(cache 全关)并发消费同一 KubeBrain,无存储瓶颈
- 原生 apiserver 在配方内完全够用;多 apiserver 是 k8s 原生 HA 形态,客户端指端点即可
- kubegateway(DispatchPolicy: Resources+UpstreamSubset)是该架构规模化后的统一入口路由层,按需引入(低优先级)

## 附:三机终版拓扑与点火配方

```
.13  TiKV+PD(149G)+ KubeBrain(仅 failover 应急,勿常任 leader)
.14  KubeBrain leader + 主 apiserver(巨型 cache 全关,MemoryMax 150G)
.12  pods-shard apiserver(MemoryMax 330G)+ KWOK×6 + kcm/scheduler(时分复用)
```

apiserver 关键 flags:
```
--disable-admission-plugins=ServiceAccount,NamespaceLifecycle,ValidatingAdmissionPolicy,MutatingAdmissionPolicy,MutatingAdmissionWebhook,ValidatingAdmissionWebhook,ResourceQuota,LimitRanger,RuntimeClass,DefaultStorageClass,PodSecurity,CertificateApproval,CertificateSigning,CertificateSubjectRestriction
--feature-gates=WatchList=false,WatchListClient=false,SizeBasedListCostEstimate=false
--watch-cache-sizes=secrets#0,deployments.apps#0,replicasets.apps#0,pods#0,services#0   # 主 apiserver;pods-shard 反转 pods
--etcd-compaction-interval=6h --etcd-count-metric-poll-period=0
--etcd-servers=<双端点>
```
kcm:`--controllers=deployment-controller,replicaset-controller --feature-gates=WatchListClient=false`(无 SA private key)
