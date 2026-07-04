# Watch 重连惊群 / Reconnect Herd (#30)

**问题**:leader 故障转移(或 watch 事件缓冲区溢出触发 `watchCache.Reset()`)后,watch-cache ring 是**冷的**。此时 apiserver 重连把每个 watch 都从各自的旧 revision 重新建立,几乎全部 miss ring → 落到 `historyWatchEvents` 的**全前缀、全版本存储扫描**(KubeBrain 键布局是 userKey-first、无 revision 有序索引,所以"某 revision 以来的事件"只能全扫再客户端过滤 —— 这是它写扩展性/无热点的代价)。这些扫描被 `historyScanConcurrency=8` 限流。

## 实测(2026-07-04,`hack/watchgen`)

干净堆 leader、冷 ring 下,N 个 watcher 同时从旧 revision 重连:

| N | 全部追上 | p50 | p99 | max |
|---|---|---|---|---|
| 1 | 1/1 | 918ms | 918ms | 918ms |
| 8 | 8/8 | 1.04s | 1.04s | 1.05s |
| 16 | 16/16 | 1.01s | 1.94s | 1.95s |
| 50 | 50/50 | 3.79s | 6.41s | 6.41s |
| 100 | 100/100 | 6.18s | **11.4s** | 11.4s |

**结论**:惊群**优雅**(100/100 全追上、0 错误、0 重启、0 泄漏、goroutine 便宜多路复用),但催迟随 N 线性 **~1s×⌈N/8⌉** —— 纯 8-slot 串行化。故障转移后 watch 重建有 O(N/8) 秒拖尾。

> 注:早前一次看似"硬挂死"是被 leader 4GB 堆(删除尖峰残留 Go 堆)的 GC 抖动放大;重启到 370MB 后才看清是"优雅但串行"。测惊群务必用干净堆。

## 采用的修复:B —— 共享扫描 singleflight

`pkg/backend/history_scan.go` 的 `scanGroup`(手写 singleflight,`x/sync/singleflight` 未 vendor)把**同一 `(prefix, fromRevision)`** 的并发历史扫描折叠成**一次**共享扫描,结果只读复用。要点:

- singleflight 的 key 是 `prefix + "\x00" + fromRevision`。共享扫描发的正是调用者要的窗口 `[fromRevision, currentRevision]` —— **不**放宽到压缩水位。放宽本可让更多调用者共享,但没有活跃 compactor 时水位是 0,会退化成"从 revision 1 发所有版本"的**无界** O(全历史)事件列表(既永不返回、又违反超大规模不变量)。保持窗口在 fromRevision 把发送量限定在调用者真正要的范围;全前缀 `Iter` 的读取量两种写法一样。
- 只有 singleflight 的**执行者**占用一个 `historyScanSem` 槽 —— 一个 `(prefix,rev)` 上的惊群只花 1 个槽,不是 N 个。
- 取消安全:等待者只等自己的 ctx;执行者若因自身 ctx 失败,其它等待者用自己的 ctx 重跑,一个断连不会连累别人。

**效果**:同 `(prefix, fromRevision)` 惊群的存储代价 **O(N) → O(1)**。单测 `history_scan_test.go`:50-watcher 惊群 → 可证**仅 1 次** Iter;等待者遵守自身 ctx。`./pkg/backend` 全套 + `-race` 绿。

**实测验证(kind + TiKV,冷 ring,/registry 含 40k key,节流写入)**:100-watcher 惊群 catch-up 的 max 从**无 B 的 30.6s(随 N 线性,~2.3s×⌈N/8⌉)**降到**有 B 的 ~4.5s(基本随 N 持平)** —— 100 个 watcher 共享一次扫描而非串行 13 批。趋势从"线性"变"持平"即为修复生效的证据。

## 未采用:A —— leader 启动预热 ring(评估后否决)

预热 ring 需要"最近事件按 revision 有序" —— 但 count-index 重建用的是 `List`(每键最新存活版本、按**键**序),喂不了按 revision 排序的事件流。因此真正的预热得在**每次 leader 启动**多做一次**全键空间、全版本、按 revision 排序**的扫描:

- 这是一次**新的 O(全键)扫描**,与"超大规模、避免 O(全键)热路径"(北极星 #1)正面冲突;
- 会**~翻倍故障恢复时间**(count-index 重建本就 O(全键)),而我们恰恰要**快速故障恢复**(#2);
- 收益已被 B 大部分吃掉(残余仅 ~几秒、且优雅)。

**净结论**:B(共享扫描)是正解 —— 尊重 #1、不拖慢故障恢复、把惊群尖锐部分 O(N)→O(1)。A 的全扫预热得不偿失,否决。

（B 的 line-scale 实测验证待一次重新部署;目前为单测 + -race 验证。）
