# P1 读放大 — 压测基线（改动前）

在改任何代码前用官方 etcd 客户端 + Prometheus 指标量化读放大，作为对照基线。
harness：`hack/etcd-client-compat/read_amp_bench_test.go`（`TestReadAmpBaseline`）。

## 方法
- 打**单个 leader pod**（port-forward）以干净归因后端存储往返：
  ```
  kubectl -n kubebrain-dev patch deploy kubebrain --type=json \
    -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--enable-storage-metrics=true"}]'
  L=$(for p in $(kubectl -n kubebrain-dev get po -oname|grep kubebrain); do \
      kubectl -n kubebrain-dev logs $p --tail=1500 2>/dev/null|grep -q "start leading" && echo ${p#pod/}; done)
  kubectl -n kubebrain-dev port-forward pod/$L 13379:3379 18080:8080 &
  cd hack/etcd-client-compat
  READ_AMP=1 ENDPOINT=127.0.0.1:13379 READ_AMP_METRICS_URL=http://127.0.0.1:18080/metrics \
    READ_AMP_N=2000 go test -run TestReadAmpBaseline -v -timeout 20m ./...
  ```
- 关键指标 `storage_iter_start`：后端 `KvStorage.Iter` 每次调用 +1。LIST 每返回一个 KV 触发一次 `GetEtcdMetadata`→一次 Iter，故"iter/返回KV"≈读放大倍数。

## 基线结果（N=2000，~1KB 值，page=500，TiKV 后端，2026-07-02）

| 操作 | 返回 | 墙钟 | 后端 Iter 次数 | iter / 返回KV |
|---|---|---|---|---|
| 单页 LIST (limit=500) | 500 | **9.99s** | 502 | **1.00** |
| 全量分页 LIST (2000, 4 页) | 2000 | **40.5s** | 2007 | 1.00 |
| CountOnly | 2000 | 61ms | 1 | — |
| 历史 LIST @rev (2000) | 2000 | 39.9s | 2007 | 1.00 |
| Watch 扇出 (20 watcher × 100 写) | 2000 事件 | 12.7s | 6300 | **3.15 / (写×watcher)** |

## 结论（对应审核项）
- **#3 / #7**：LIST/Get 每返回一个 KV 都多一次后端存储 Iter（取 create_revision/version 元数据），且在 TiKV 上每次 Iter 含一次取 TSO + 反向快照扫描 → **~20ms/键**。单页 500 键 = 10s；100k pod 的 LIST 外推 ≈ 2000s。这是"规模超越 etcd"目标的最大障碍。
- **#10 / #28**：watch 扇出每事件每 watcher ~3 次存储读（元数据 + prev-kv，含重试预算）。W 个 watcher × E 事件 = O(W×E) 存储读。
- **#5 / #27 / #29**：分页/历史 LIST 的精确 Count 与 CountOnly 走全量扫描（此规模下墙钟未爆，但 O(N²) 特性需更大 N 才显现；已在 harness 覆盖，后续放大 N 复测）。

## 目标（改动后用同一 harness 复测）
- LIST 的 `iter/返回KV` 从 ~1.0 降到 ~0（元数据内联或一次批量解析，消除 per-KV 往返）。
- 单页 LIST 墙钟从 ~10s 降到亚秒级。
- watch 扇出 `iter/(写×watcher)` 从 ~3 降到 ~0（事件产生时解析一次，而非每 watcher 重复读）。
