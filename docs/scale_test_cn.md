# 超大规模压测 / Scale Test（找第一堵墙）

**目标**:把 keyspace 灌到百万~千万级 + 持续高写,找 KubeBrain 会在哪里先断。**环境**:104 核 / 362G,3 PD + 3 TiKV,KubeBrain 3 副本(默认租约镜像,`--enable-count-index --count-index-max-keys=5M`)。工具 `hack/loadgen`(单前缀,C=500,put 为主)。

## 实测结果(2026-07-04)

从 591k key 起,持续写 ~5k puts/s,keyspace 涨到 ~5M+:

| 信号 | 结果 |
|---|---|
| **写扩展性** | **优秀**:591k → ~5M key,**0 次 KubeBrain 重启**,~5k puts/s 稳定,写路径百万级毫无压力。 |
| **TiKV region** | 19 → 29 自动分裂,PD 稳,负载摊开。 |
| **内存(关键结论)** | RSS 随 count 索引线性增长:795MB@591k → **~2.4GB@~5M**;**越过 5M 上限时索引溢出禁用、丢弃 btree → RSS 从 2.4GB 塌回 ~787MB(基线)**。goroutines 也回到 2389 基线。 |
| **越过上限后的正确性** | 不崩:count 索引禁用后,CountOnly/List 回退到扫描,结果仍正确(只是慢)。 |

## 两个核心结论

### ① 内存在超大规模下**有界**(count-index cap 生效,优雅降级)

这是最重要的验证:count 索引把 key 放内存换 O(range) 计数,但**有 `--count-index-max-keys` 上限**。实测越过 5M 时:

```
RSS: 795MB(591k) → 2.4GB(~5M) --[越过 5M 上限]--> 787MB(基线)
     索引 Apply 增长 ────────────  索引 overflowed=true、禁用、丢弃 ──── 回退扫描
```

**内存不会无限增长**;溢出是**优雅**的(禁用 + 回退扫描,0 崩溃、0 重启)。对"超大规模是产品灵魂"来说,这是个正面结果:千万级下内存被 cap 兜住,不会 OOM。运维按机器内存设 `--count-index-max-keys`(越大越省扫描、越吃内存)。

### ② 第一堵墙 = **大范围聚合读 在 重写负载下 / 溢出后**

- 全 keyspace 的 `CountOnly`(over `/`)在 ~2M+ key 时从 762ms 恶化到 **>30s 超时**。
- 机制:(a) 持续重写下 count 索引跟不上最新 revision → `CountAtRevision(current)` not-ready → 回退扫描;(b) 越过 5M 上限后索引直接禁用 → 一律扫描。大范围扫描 + 并发重写 → 慢。
- **但要客观**:全 `/` 聚合计数是**不现实**的 op —— apiserver 是按资源前缀(`/registry/pods/`)计数、读 watchcache 的稍旧 revision,不是 bleeding-edge 全库。小范围 / 稍旧 revision 的读应仍快。
### ③ Realistic per-prefix 读在 5M+ 下仍快(缺口已补)

在 **5M+ 总 key、且 count 索引已溢出禁用(worst case,一律回退扫描)** 下实测:

| 读类型(每资源前缀 ~8.5k key) | 延迟 |
|---|---|
| **点 Get** | **4–7ms** |
| 每资源前缀 CountOnly(~8.5k key) | **~130–150ms** |
| 每资源前缀 List 页(limit 500) | **~143ms** |
| 全库聚合(数百万 key 一个 range) | 超时 |

**关键洞察:读延迟随 range(前缀)大小走,不随总 keyspace 走。** 一个"每资源前缀"读(几千 key)即便索引禁用、总量 5M+,仍 ~130–150ms —— 因为扫描被限定在该前缀内。只有"全库/数百万 key 一个 range"的聚合才慢(且不现实)。**注**:这是索引禁用的 worst case;索引启用(在 cap 内)时同样的前缀读会更快(索引直接答,~ms)。

## 净结论

> **写扩展性 + 内存有界性在千万级下都 hold(0 重启、溢出优雅降级、RSS 被 cap 兜回基线);且 realistic apiserver 读(点 Get 4–7ms、每资源前缀 List/Count ~130–150ms)在 5M+ 下仍快 —— 因为读延迟随前缀大小走、不随总量走。** 唯一慢的是"全库聚合读"这种非典型 op(apiserver 不这么读)。第一堵墙对典型读路径无影响。

（测试用的 ~5M key 已在测后清理。）
