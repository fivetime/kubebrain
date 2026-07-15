# 官方形态直连 benchmark(2026-07,#61/#73)

**工具**:etcd 官方 `tools/benchmark` + `hack/scale-lab/bigstream`(delete 阶段)。
**形态**:300 gRPC client / 100 conn 直连 KubeBrain(绕开 apiserver),70B key / 512B value。
**环境**:3 机混部(TiKV+PD+KubeBrain 各 3 副本同机,1GbE 互连),存量 1650 万 k8s 对象;**安静集群**(k3s/KWOK 停,无背景负载)。客户端在 .13,目标 leader。

## 结果(安静集群,最终版)

| 场景 | QPS | p50 | p99 |
|---|---|---|---|
| **PUT(insert,顺序键)** | **4788/s** | 54ms | 182ms |
| **DELETE(逐 key,300 并发)** | **5470/s** | 45ms | 167ms |
| 单 key GET(leader,300 并发) | **8404/s** | 20ms | 192ms |
| 单 key GET(leader,clients=1 本征) | 190/s | **5.2ms** | 7.3ms |
| 单 key GET(follower,clients=1,+read-index 一跳) | 106/s | 9.3ms | 14.5ms |
| 前缀 count-only(490 万键,50 并发,index-served) | 126/s | 364ms | 585ms |
| 前缀 Range limit=1(490 万键 range,300 并发) | 79/s | 3.7s | 4.6s |

写路径吞吐由 TiKV quorum 写决定;单点读本征 5.2ms(TSO ~0.5ms + read-rev 同步 + TiKV 点查 + 2 跳网络),与历史 50 万级终验的 9.2ms(follower 形态)一致——**规模从 50 万涨到 1650 万,点读本征成本没有退化**。

## #73 侦破记:47.5ms"读延迟退化"其实是自伤

首轮测量(k3s+KWOK 在线)读出 leader 点读 p50 294ms,停掉背景负载后仍有 47.5ms(clients=1),一度怀疑"TiKV 读池背景税"/“PD TSO 异常”。专项定位(#73)结果:

1. PD server 侧 TSO 处理仅 **5.85µs/次**(`pd_server_handle_tso_duration`,处理点在 `pd/server/grpc_service.go` 只包住内存分配);client 侧 15.1ms(`pd/client/tso_request.go`,含批处理排队+网络往返)——差值全在网络。
2. 三机互 ping RTT 均值 9-10ms、丢包 0.8-5%,而 min 0.17ms;分布近似均匀铺在 0-15ms——典型**链路打满后的队列积压(bufferbloat)**,1GbE 实测 0.91Gbps 线速被占满。
3. 洪水源头 = **上一轮遗留的 benchmark 进程自己**:`range limit=1` 300 并发扫 490 万键前缀,叠加当时 leader 刚重启、count index 重建中 → count-only 走 follower 本地全扫回退,合计以 ~1.8Gbps 持续从 TiKV 拉数据 58 分钟(storage_scan 指标:22901 次 worker 扫描、累计 592GB)。
4. 杀掉残留进程后 30 秒:RTT 0.52ms/零丢包,点读 47.5ms→**5.2ms**,300 并发 GET 574/s→**8404/s**。

教训:混部 1GbE 集群上,任何一个满线速的扫描消费者都会通过网络队列把**全集群所有 RPC**(TSO/raft/读写)拖进 5-15ms 排队;先看网卡吞吐再怀疑存储引擎。

## 两个结构性发现

1. **高并发小-limit Range 的信号量队头(P3)**:`globalScanWorkers=24` 对每请求只取 1 行的 limit=1 扫描与全量扫描一视同仁排队;300 并发 × 每请求 3-4 分区 worker ≈ 千级排队 → 安静集群上 p50 仍 3.7s。且分区 worker 从发起到被 merger 取消平均每请求多拉 **~30MB**(592GB/2 万请求),取消传播越慢放大越狠。**k8s 流量不触发此形态**(apiserver 分页 limit=500 且并发低、GET 走单 key 快路径);若未来有高并发直连小扫描消费者,修法=小 limit 独立小信号量 + 首批扫描行数钳到 limit。
2. **count-only 已由 count index 承接**(本轮 1000 次查询 miss 零增长),364ms p50 为 read-rev 同步 + 50 并发排队,非全扫;index 未就绪时 follower 的本地全扫回退在高频 count 下代价巨大(见上,#41 的已知设计权衡)。

## 复现

```
# 写入
etcd-benchmark put --endpoints=http://<leader>:3379 --clients=300 --conns=100 \
  --key-size=70 --val-size=512 --total=100000 --sequential-keys \
  --key-space-size=1000000 --prefix=/bench73/
# 读
etcd-benchmark range /registry/namespaces/kube-system --total=100000 --clients=300 --conns=100 ...
etcd-benchmark range /registry/deployments/ /registry/deployments0 --count-only --total=1000 --clients=50 ...
# 删除(顺便清场)
bigstream -mode delprefix -prefix /bench73/ -workers 300 -endpoint <leader>:3379
```
