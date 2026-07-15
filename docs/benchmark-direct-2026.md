# 官方形态直连 benchmark(2026-07,#61)

**工具**:etcd 官方 `tools/benchmark` + `hack/scale-lab/bigstream`(delete 阶段)。
**形态**:300 gRPC client / 100 conn 直连 KubeBrain(绕开 apiserver),70B key / 512B value,50 万 insert + 50 万 delete。
**环境(重要,数字须带条件读)**:3 机混部(TiKV+PD+KubeBrain 各 3 副本同机),**存量 1650 万 k8s 对象,且 k3s+KWOK 背景负载在线**(含 k3s 1.36 默认开启的 DetectCacheInconsistency 常驻对账扫描)——这是"带着生产陪跑者"的数字,不是空库极限。客户端在 .13,目标 leader .15。

## 结果

| 场景 | QPS | p50 | p99 |
|---|---|---|---|
| **PUT(insert,顺序键)** | **4750/s** | 58ms | 172ms |
| **DELETE(逐 key,300 并发)** | **2308/s** | 125ms | 405ms |
| 单 key GET(leader,混部重载机) | 574/s | 294ms | 3.8s |
| 单 key GET(空闲 follower,+read-index 一跳) | 1637/s | 198ms | 440ms |
| 前缀 Range limit=1(50 万键 range) | 290/s | 966ms | 2.4s |
| 前缀 count-only | 196/s | 238ms | 480ms |

写路径(put/delete)延迟稳定、吞吐由 TiKV quorum 写决定;delete 较 put 慢一倍符合其 read-modify-write 本质(读旧值+写墓碑+事件日志)。

## 两个结构性发现

1. **高并发小-limit Range 的信号量队头**:`globalScanWorkers=24`(#40 的读池保护)对每请求只读 2 行的 limit=1 扫描与全量扫描一视同仁排队;300 并发 × 每请求 3-4 分区 worker ≈ 千级排队 → p50 逼近 1s。**k8s 流量不触发此形态**(apiserver 分页 limit=500 且并发低、GET 走单 key 快路径),故定级 P3 记录;若未来有高并发直连小扫描消费者,可为小 limit 请求独立小信号量。
2. **读延迟被背景对账扫描抬高**:同形态单 key GET 在空闲 follower 上快 3 倍,但仍百 ms 级——TiKV region-leader 读池被 k3s 的 DetectCacheInconsistency 常驻扫描与 KWOK churn 占用,所有点查排队。这是 `survival-stage0-cn.md` 中"必关该 gate"的又一实证。**干净环境重测预期读延迟低一个数量级**(TiKV 点查本征 ~1-2ms)。

## 复现

```
# 写入
etcd-benchmark put --endpoints=http://<leader>:3379 --clients=300 --conns=100 \
  --key-size=70 --val-size=512 --total=500000 --sequential-keys \
  --key-space-size=1000000 --prefix=/bench61/
# 读
etcd-benchmark range /bench61/ /bench61z --limit=1 --total=100000 --clients=300 --conns=100 ...
etcd-benchmark range /bench61/ /bench61z --count-only --total=1000 --clients=50 ...
# 删除(顺便清场)
bigstream -mode delprefix -prefix /bench61/ -workers 300 -endpoint <leader>:3379
```
