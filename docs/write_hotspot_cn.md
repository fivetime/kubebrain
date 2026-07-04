# 写热点 / Write Hotspot（KubeBrain over TiKV）

结论先行:**KubeBrain 的 key 布局本身没有热点缺陷;写热点是 TiKV/PD 的调优范畴,而且 TiKV 的负载分裂(load-based split)在高 QPS 下会自动把热点 region 打散、把延迟压住。** 定向实验实测:单前缀 5117 ops/s、p99=252ms、0 错误,热点 region 从 12 自动分裂到 19。

## 为什么 KubeBrain 的 key 布局没有热点缺陷

对象键编码是 **`{magic}{userKey}${revision:8BE}`**(`pkg/backend/coder/normal.go`):**userKey 在前,revision 在后缀**。含义:

- 键按 **userKey** 排序 → **没有**"全局单调 revision 压在一个 region 头部"那种 naive 热点(如果编码成 `{revision}{key}` 就会有,KubeBrain 没这么干)。
- k8s 的写天然按前缀聚簇(pods 全在 `/registry/pods/`、configmaps 全在 `/registry/configmaps/` …)。**一批写集中在同一 prefix** → 落在覆盖该 prefix 的 region 上,直到 TiKV 分裂它。这是 k8s 工作负载的固有特征,不是 KubeBrain 的问题。

## TiKV 什么时候会分裂热点 region(默认阈值)

（本集群实测,`kb-tikv-0:20180/config`）:

| 参数 | 默认值 | 触发分裂的条件 |
|---|---|---|
| `coprocessor.region-split-size` | 256 MiB | region **体积**超过时按大小分裂 |
| `split.qps-threshold` | **3000** | region 单点 **QPS** 超过时按负载分裂 |
| `split.byte-threshold` | ~30 MB/s | region 单点**写字节吞吐**超过时按负载分裂 |
| `split.region-cpu-overload-threshold-ratio` | 0.25 | region CPU 过载比例 |

KubeBrain 整个 keyspace 本集群只有 ~245MB / 12 region(~20MB/region),**远低于 256MB 体积阈值** → 不会按大小分裂。所以热点 region 是否被打散,**取决于负载分裂**(是否跨过 3000 QPS / 30MB/s)。

## 定向实验(2026-07-04,104 核 / 3 TiKV)

`hack/loadgen -ep <kb> -prefix /hotspot-probe -c 400 -d 100`(单前缀高并发写):

- **吞吐 5117 ops/s、511717 ops、0 错误**;
- **p50=57ms、p99=252ms、max=557ms**(延迟低且稳);
- **region 12 → 19**:TiKV 的负载分裂在写压集中时**自动把热点 region 打散成 ~7 个**,把负载摊开。

即:**高 QPS 下 TiKV 自动处理热点,KubeBrain 单前缀 5117 ops/s 仍 sub-300ms p99。** 这也复核了之前"满载天花板 ~1-2s 延迟"**不是**纯粹的单 region 热点未分裂 —— 那次是 configmap CRUD(每对象 3 次写)+ 整个 k3s 集群并发写 + 累计直方图的合成,不是这里的干净单前缀写。

## 什么时候仍需要调优(以及怎么调)

**残留场景**:一个前缀负载**中等**(低于 3000 QPS 负载分裂阈值)但对**延迟敏感**时,该 region 不会分裂,可能比理想略高延迟。此时是 **TiKV 侧调优**,不是改 KubeBrain:

- **降低 `split.qps-threshold`**(如 500~1000)让热点更早分裂 —— 对延迟敏感、前缀聚簇的 k8s 负载最有效。
- **降低 `region-split-size`**(更多 region、更高并行)—— 代价是 region 数变多、调度开销上升。
- **`enable-region-bucket`**:region 内更细粒度的负载/读分布。
- **预分裂(pre-split)**:对已知热前缀(如 `/registry/events/`、`/registry/leases/`)在 bootstrap 时预先切分。可由运维用 `pd-ctl operator add split-region` 或 TiKV split API 做;KubeBrain 侧自动预分裂属可选特性,当前不做(遵循"性能/稳定优先、不盲目加功能")。

TiUP/TiDB Operator 里改 `TidbCluster` 的 `spec.tikv.config`:

```toml
[split]
qps-threshold = 1000        # 默认 3000;对前缀聚簇 + 延迟敏感的 k8s 负载调低
byte-threshold = 10485760   # 默认 ~30MB/s;按需调低
```

## 净结论

> **写热点不是 KubeBrain 的代码缺陷** —— key 布局 userKey-first 已避免全局热点;剩余的前缀聚簇热点由 **TiKV 负载分裂**自动处理(高 QPS 实测 5117 ops/s / p99 252ms / 12→19 region)。**对延迟敏感的中等负载,调 TiKV `split.qps-threshold` 即可,无需改 KubeBrain。**
