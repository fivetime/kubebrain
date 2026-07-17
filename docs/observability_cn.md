# 可观测性 / Observability（KubeBrain over TiKV）

KubeBrain 已内置**标准 Prometheus** 指标(真 registry + `promhttp.Handler()`),挂在 **info 端口(`--info-port`,默认 8080)的 `/metrics`**。暴露方式与"进程怎么被调度"无关,所以静态 Pod 部署不影响可观测。

## 如何抓取(静态 Pod 数据存储的正确姿势)

生产里 KubeBrain 是 apiserver 的数据存储,通常以**静态 Pod**(kubelet 管,像 etcd)或 systemd/容器运行 —— 不能是被同一个 apiserver 管的 Deployment(先有鸡还是先有蛋)。抓取和 etcd 完全同构,三选一:

1. **按节点 IP 的 static / file_sd(推荐,带外)**:把 `nodeIP:8080` 写进 Prometheus 的 static_config/file_sd。**不依赖 apiserver** → KubeBrain / apiserver 挂了照样能抓到 metrics,这正是数据存储层该有的独立监控。
2. **mirror pod + kubernetes_sd(pod role)**:kubelet 会为静态 Pod 发布只读 mirror pod(带 podIP),集群内 Prometheus 发现后抓 `podIP:8080/metrics`。正常态仪表盘用它即可。
3. **手工 Service + Endpoints + ServiceMonitor**:即 kube-prometheus 给 etcd 用的那套。

> **要点:不要让 KubeBrain 的监控依赖它自己服务的那个 apiserver。** 首选带外(方式 1),否则 KubeBrain 一挂你恰好在最需要 metrics 时瞎了。info 端口支持 TLS + client-cert(`--info-cert-file` 等,#32),抓取端带证书即可。

## 关键 SLI

| 指标 | 含义 / 用途 |
|---|---|
| `read` / `write` counter(labels: `method`,`success`,**`errclass`**) | 请求量 + 成败 + **错误分类**。`errclass` 区分良性可重试(`revision`=压缩/未来 revision、`unavailable`=换主、`fenced`=写栅栏)和真故障(`deadline`=超时/过载、`other`=意外)。 |
| `read.latency` / `write.latency` histogram(labels: `method`,`success`) | 读写延迟分布 → p99 告警抓延迟/读放大回归。 |
| `storage_batch_count`(op=`write_batch`)histogram | 存储层批量写延迟(TiKV TSO+raft)。 |
| `leader_revision` gauge | 当前 leader 的 revision(是否推进)。leader 身份见 info 端口 `/election`。 |
| `count_index.keys` gauge | 活跃 key 数(对象量)—— 容量/膨胀的**便宜**信号(需 `--enable-count-index`)。 |
| `watch.collector.stalled` / `watch.collector.skipped_revision` counter | 事件收集器 stall/自愈跳过 —— 正常应为 0,非 0=有 writer 死在 deal↔notify 之间。 |
| `lease.orphan_sweep.{key_deleted,record_reclaimed,err}` counter | 孤儿 lease 清扫活动 —— 正常应极低。 |
| `write.fence.reject` counter | 写栅栏拒绝(#39)—— 换主瞬间少量正常;持续高=leader 抖动。 |
| `grpc_server_admission_inflight` gauge | 当前公开 client RPC 总并发，stream 在完整生命周期内持续占槽。 |
| `grpc_server_admission_rejected` counter(labels: `method`,`kind`) | `--max-requests-inflight` 超限拒绝数；持续增长表示实例过载或限额过低。 |
| `grpc_server_rate_limit_rejected` counter(labels: `method`,`kind`) | `--max-request-rate` token bucket 超限数；`kind=unary` 统计 unary RPC，`kind=stream_message` 统计 Watch/KeepAlive 等每条入站消息。 |
| `delete_range_admission_rejected` counter | `DeleteRange` 命中 `--max-delete-range-keys` 上限的前置拒绝数；拒绝不会分配 revision 或部分删除。 |
| `watch_admission_active` gauge | 当前进程已接纳的逻辑 Watch 数；同一 gRPC stream 内 multiplexed Watch 逐个计数。 |
| `watch_admission_rejected` counter | `--max-watches` 超限拒绝的 Watch create 数；持续增长表示 Watch 负载超过实例预算或限额过低。 |
| `etcd_server_healthcheck` gauge(labels: `type`,`name`) | etcd 兼容的 `/livez`、`/readyz` 分项状态；1=最近一次成功，0=最近一次失败。 |
| `etcd_server_healthchecks_total` counter(labels: `type`,`name`,`status`) | 分项检查累计结果；用 `rate(...{status="error"}[5m])` 区分后端不可读与无 leader。传统 `/health` 同时提供 `etcd_server_health_success`/`failures`。 |

## 推荐告警(方向,阈值按环境调)

- **leader 频繁切换 / 抖动**:`rate(etcd_server_healthchecks_total{type="readyz",name="linearizable_read",status="error"}[5m]) > 0` 持续出现，或 `write.fence.reject` 持续增长，或 `/election` leader 地址频繁变。正常换主不应导致 Pod 重启；重启率 > 0 是独立的进程稳定性告警。→ 见 [[failover_tuning_cn.md]](租约调优)。
- **共享存储不可读**:`etcd_server_healthcheck{type="livez",name="serializable_read"} == 0`，或对应 error counter 持续增长。它不依赖 KubeBrain leader，可直接指向 TiKV/PD、网络或租户 keyspace 读路径。
- **写延迟过高**:`write.latency` p99 持续 > 你的 lease-sensitive 控制器续期窗口的一半(默认 controller-manager ~15s → 阈值 ~7s;满载 TiKV 单 region 热点会推高)。
- **真故障率上升**:`rate(read/write{errclass="other"})` 或 `{errclass="deadline"}` 上升(把 `revision`/`unavailable`/`fenced` 排除 —— 那些客户端自愈)。
- **client admission 饱和**:`grpc_server_admission_inflight` 长期贴近配置上限且 `rate(grpc_server_admission_rejected[5m]) > 0`。先按 method/kind 区分长 watch 与 unary 洪峰，再扩容或调整经压测证明的限额。
- **client 请求速率饱和**:`rate(grpc_server_rate_limit_rejected[5m]) > 0`。按 method/kind 判断 unary 洪峰或单条 multiplexed stream 消息洪峰；确认不是异常客户端后，再扩容或按 TiKV 延迟和实例 CPU 压测调整 rate/burst。
- **范围删除超限**:`rate(delete_range_admission_rejected[5m]) > 0`。确认调用方是否误用了无界前缀删除；确需调大时先按 key/value 大小在独立 TiKV 上验证事务大小和 p99。
- **logical watch admission 饱和**:`watch_admission_active` 长期贴近 `--max-watches` 且 `rate(watch_admission_rejected[5m]) > 0`。先排查客户端重复建立/未取消 Watch，再扩容或按内存与事件延迟压测调整限额。
- **watch-cache 冻结**:apiserver 侧 `Too large resource version` / `Unable to sync caches`(进度通知已修,应为 0);KubeBrain 侧 `watch.collector.stalled` > 0。
- **版本膨胀**:`count_index.keys` 长期单调上涨且无压缩回落 → 检查 apiserver 压缩循环是否正常(KubeBrain 自身不自动压缩)。

## DbSize 为什么是 1 字节哨兵(而不是真实容量)

`Maintenance.Status.DbSize`/`DbSizeInUse` **有意返回相等的 1 字节哨兵**。etcd 的 DbSize 之所以重要,是因为 etcd 有硬配额(`--quota-backend-bytes`,超了变只读 NOSPACE)→ 需在撞墙前告警 + defrag。**TiKV 没有单逻辑库 bbolt 配额**,这套语义不适用;合成容量会制造假配额告警。不能返回 0,因为官方 etcdctl 3.7 的 `endpoint status -w table` 会计算 `DbSizeInUse/DbSize` 并除零 panic。1/1 只表达“无 etcd fragmentation 可报告”,不表示实际占用。

- **要字节/磁盘水位** → 抓 **TiKV/PD 自己的 Prometheus 指标**(store size、region count)。
- **要对象数** → KubeBrain 的 `count_index.keys`(便宜、现成)。
- **defrag / Alarm**:no-op(TiKV 自压缩,无 NOSPACE)。
