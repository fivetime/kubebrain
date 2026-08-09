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
| `etcd_server_request_duration_seconds` histogram(labels: `type`,`success`) | etcd upstream 兼容的请求端到端耗时，覆盖 KV `Range`/`Txn`/`Put`/`DeleteRange` 等类型；用于复用 etcd dashboard/runbook，并按 `success=false` 定位失败请求延迟。 |
| `storage_batch_count`(op=`write_batch`)histogram | 存储层批量写延迟(TiKV TSO+raft)。 |
| `leader_revision` gauge | 当前 leader 的 revision(是否推进)。leader 身份见 info 端口 `/election`。 |
| `count_index.keys` / `count_index.overflowed` gauge | leader 活跃 key 数与索引是否超过 `--count-index-max-keys`；overflow 后正确回退 TiKV 全扫，但 List/count 延迟会明显恶化。followers 的 keys=0 是正常值。 |
| `count_index.rebuild.err` counter | leader 切换时索引快照重建失败次数；非零表示 CountOnly 暂时回退 TiKV 全扫。 |
| `watch.collector.stalled` / `watch.collector.skipped_revision` counter | 事件收集器 stall/自愈跳过 —— 正常应为 0,非 0=有 writer 死在 deal↔notify 之间。 |
| `lease.orphan_sweep.{key_deleted,record_reclaimed,err}` counter | 孤儿 lease 清扫活动 —— 正常应极低。 |
| `write.fence.reject` counter | 写栅栏拒绝(#39)—— 换主瞬间少量正常;持续高=leader 抖动。 |
| `grpc_server_admission_inflight` gauge | 当前公开 client RPC 总并发，stream 在完整生命周期内持续占槽。 |
| `grpc_server_admission_rejected` counter(labels: `method`,`kind`) | `--max-requests-inflight` 超限拒绝数；持续增长表示实例过载或限额过低。 |
| `grpc_server_rate_limit_rejected` counter(labels: `method`,`kind`) | `--max-request-rate` token bucket 超限数；`kind=unary` 统计 unary RPC，`kind=stream_message` 统计 Watch/KeepAlive 等每条入站消息。 |
| `etcd_network_client_grpc_received_bytes_total` / `etcd_network_client_grpc_sent_bytes_total` counter | etcd upstream 兼容的公开 client gRPC payload 字节数；只统计 client listener，不统计 peer forwarding listener。 |
| `delete_range_admission_rejected` counter | `DeleteRange` 命中 `--max-delete-range-keys` 上限的前置拒绝数；拒绝不会分配 revision 或部分删除。 |
| `watch_admission_active` gauge | 当前进程已接纳的逻辑 Watch 数；同一 gRPC stream 内 multiplexed Watch 逐个计数。 |
| `watch_admission_rejected` counter | `--max-watches` 超限拒绝的 Watch create 数；持续增长表示 Watch 负载超过实例预算或限额过低。 |
| `etcd_debugging_server_watch_send_loop_watch_stream_duration_seconds` / `etcd_debugging_server_watch_send_loop_watch_stream_duration_per_event_seconds` histogram | etcd upstream 兼容的 Watch data response 发送耗时；watch cache init 或大批事件发送慢时用它区分 server send loop 与后端事件采集。 |
| `etcd_debugging_server_watch_send_loop_control_stream_duration_seconds` / `etcd_debugging_server_watch_send_loop_progress_duration_seconds` histogram | etcd upstream 兼容的 Watch control/progress response 发送耗时；用于定位 create/cancel/progress 通知卡在 wire send 或流控上的问题。 |
| `etcd_debugging_mvcc_pending_events_total` gauge / `etcd_debugging_mvcc_events_total` counter | etcd upstream 兼容的 Watch event backlog / delivered event 观测；pending 统计本 member 已转换但尚未交给 watch send loop 的本地 backend WatchResult 事件数，events 统计成功交付到 watch response 层的事件数。 |
| `etcd_mvcc_hash_duration_seconds` / `etcd_mvcc_hash_rev_duration_seconds` histogram | etcd upstream 兼容的 Maintenance Hash/HashKV 成功计算耗时；用于定位诊断 hash 受 keyspace、revision/compaction 查找或 TiKV 读路径影响。 |
| `etcd_debugging_mvcc_db_compaction_last` gauge / `etcd_debugging_mvcc_db_compaction_total_duration_milliseconds` histogram / `etcd_debugging_mvcc_index_compaction_pause_duration_milliseconds` histogram / `etcd_debugging_mvcc_db_compaction_keys_total` counter | etcd upstream 兼容的物理 compaction 观测；KubeBrain 将 shared-storage version GC 总耗时映射到 db compaction total，将 count-index 裁剪耗时映射到 index compaction pause，并按 full/incremental scan 实际成功删除的物理 MVCC key 累加 keys total。 |
| `etcd_debugging_mvcc_total_put_size_in_bytes` gauge | etcd upstream 兼容的本 member 成功 Put key/value 字节累计值；KubeBrain 在 public Put 和成功执行的 Txn Put op 后按实际 key+value 长度累加。 |
| `etcd_server_snapshot_apply_in_progress_total` gauge | etcd upstream 兼容的 raft snapshot apply 状态；KubeBrain 不运行 etcd raft snapshot apply，固定为 0，TiKV/PD snapshot 应看存储层指标。 |
| `etcd_server_heartbeat_send_failures_total` counter | etcd upstream 兼容的 raft leader heartbeat 发送失败计数；KubeBrain 不运行 etcd raft transport，固定为 0，TiKV/PD heartbeat 应看存储层指标。 |
| `etcd_server_proposals_committed_total` / `etcd_server_proposals_applied_total` / `etcd_server_proposals_pending` gauge，`etcd_server_proposals_failed_total` counter | etcd upstream 兼容的 raft proposal 状态；KubeBrain 不运行 etcd raft proposal pipeline，四者固定为 0。不得解释为 MVCC revision、public write RPC 或 TiKV transaction；底层共识使用 TiKV/PD 原生指标。 |
| `etcd_server_healthcheck` gauge(labels: `type`,`name`) | etcd 兼容的 `/livez`、`/readyz` 分项状态；1=最近一次成功，0=最近一次失败。 |
| `etcd_server_healthchecks_total` counter(labels: `type`,`name`,`status`) | 分项检查累计结果；用 `rate(...{status="error"}[5m])` 区分后端不可读与无 leader。传统 `/health` 同时提供 `etcd_server_health_success`/`failures`。 |

## 推荐告警(方向,阈值按环境调)

- **leader 频繁切换 / 抖动**:`rate(etcd_server_healthchecks_total{type="readyz",name="linearizable_read",status="error"}[5m]) > 0` 持续出现，或 `write.fence.reject` 持续增长，或 `/election` leader 地址频繁变。正常换主不应导致 Pod 重启；重启率 > 0 是独立的进程稳定性告警。→ 见 [[failover_tuning_cn.md]](租约调优)。
- **共享存储不可读**:`etcd_server_healthcheck{type="livez",name="serializable_read"} == 0`，或对应 error counter 持续增长。它不依赖 KubeBrain leader，可直接指向 TiKV/PD、网络或租户 keyspace 读路径。
- **写延迟过高**:`write.latency` p99 持续 > 你的 lease-sensitive 控制器续期窗口的一半(默认 controller-manager ~15s → 阈值 ~7s;满载 TiKV 单 region 热点会推高)。
- **etcd 兼容请求延迟过高**:`histogram_quantile(0.99, rate(etcd_server_request_duration_seconds_bucket[5m]))` 按 `type` 分组持续升高。它与 `read.latency`/`write.latency` 的 DBaaS-native method 维度互补，适合直接套用 upstream etcd dashboard。
- **真故障率上升**:`rate(read/write{errclass="other"})` 或 `{errclass="deadline"}` 上升(把 `revision`/`unavailable`/`fenced` 排除 —— 那些客户端自愈)。
- **gRPC 服务端故障**:
  `rate(grpc_server_handled_total{grpc_code=~"Unknown|Internal|DataLoss"}[5m]) > 0`。
  不要用 `grpc_code!="OK"`：NotFound、InvalidArgument、OutOfRange、Unavailable、
  ResourceExhausted 和 Unimplemented 都可能是正常 etcd 控制流、调用方错误、换主、
  配额保护或明确的平台替代能力。
- **client admission 饱和**:`grpc_server_admission_inflight` 长期贴近配置上限且 `rate(grpc_server_admission_rejected[5m]) > 0`。先按 method/kind 区分长 watch 与 unary 洪峰，再扩容或调整经压测证明的限额。
- **client 请求速率饱和**:`rate(grpc_server_rate_limit_rejected[5m]) > 0`。按 method/kind 判断 unary 洪峰或单条 multiplexed stream 消息洪峰；确认不是异常客户端后，再扩容或按 TiKV 延迟和实例 CPU 压测调整 rate/burst。
- **范围删除超限**:`rate(delete_range_admission_rejected[5m]) > 0`。确认调用方是否误用了无界前缀删除；确需调大时先按 key/value 大小在独立 TiKV 上验证事务大小和 p99。
- **logical watch admission 饱和**:`watch_admission_active` 长期贴近 `--max-watches` 且 `rate(watch_admission_rejected[5m]) > 0`。先排查客户端重复建立/未取消 Watch，再扩容或按内存与事件延迟压测调整限额。
- **watch-cache 冻结**:apiserver 侧 `Too large resource version` / `Unable to sync caches`(进度通知已修,应为 0);KubeBrain 侧 `watch.collector.stalled` > 0。
- **watch send loop 拥塞**:`histogram_quantile(0.99, rate(etcd_debugging_server_watch_send_loop_watch_stream_duration_seconds_bucket[5m]))` 或 control/progress send-loop p99 持续升高。若这些指标高而 TiKV/collector 正常，优先查 gRPC 流控、客户端消费速度和 apiserver watch cache 初始化并发。
- **版本膨胀**:`count_index.keys` 长期单调上涨且无压缩回落 → 检查 apiserver 压缩循环是否正常(KubeBrain 自身不自动压缩)。
- **count index 退化**:`max(count_index_overflowed) > 0` 持续 1m，或
  `increase(count_index_rebuild_err[10m]) > 0`。overflow 时应同时提高实例内存规格和
  `--count-index-max-keys`，不能只放大 key cap；重建失败先检查 TiKV scan 错误、PD
  可用性和 leader 切换频率。不要用 `count_index_keys == 0` 告警，followers 正常为 0。
- **租户逻辑容量**：启用 `--quota-backend-bytes` 后，监控
  `quota.logical_usage_bytes / quota.backend_bytes`，建议在 80% 和 90% 分级告警；
  `quota.nospace == 1` 表示已触发持久 NOSPACE。告警激活后所有 Put（包括缩小
  value）和任一分支含 Put 的 Txn 均拒绝；应通过删除或 lease revoke 使 usage 严格低于
  quota 后，再执行 `etcdctl alarm disarm`。与 etcd 一致，达到/超过 quota 时执行
  disarm 本身也会成功，但下一次含 Put 请求会立即重新激活 NOSPACE，因此不能把短暂
  清空 alarm list 当作容量已经恢复。这些指标统计当前存活 key+value 的逻辑字节，
  不包含 MVCC 历史、事件日志、lease/auth 元数据和 TiKV 副本开销。
  每个 serving 副本会立即并每 15 秒从共享 TiKV metadata 刷新这些 gauges，单次读取
  最多 5 秒；不要依赖某个固定 Pod 的瞬时值。生产规则
  `KubeBrainQuotaNoSpace`、`KubeBrainQuotaUsageHigh`、
  `KubeBrainQuotaMetricsInconsistent` 和 `KubeBrainQuotaRefreshFailures` 分别覆盖持久
  NOSPACE、90% 水位、三副本 series 缺失/不一致以及共享状态读取失败。换主或 mutation
  后一个刷新周期内的短暂不一致正常；持续超过 1 分钟表示副本无法收敛，不能仅用旧
  leader 的 stale gauge 判断 tenant 最终状态。

## DbSize 与物理容量

启用 `--quota-backend-bytes` 时，`Maintenance.Status.DbSize`/`DbSizeInUse`
返回该 keyspace 当前存活 key+value 的逻辑字节，`DbSizeQuota` 返回配置上限。未启用时
仍返回相等的 1 字节哨兵和 etcd 默认 2 GiB quota 兼容值；1/1 只表达“无 bbolt
fragmentation 可报告”，不表示 TiKV 实际占用。

- **要物理字节/磁盘水位** → 抓 **TiKV/PD 自己的 Prometheus 指标**(store size、region count)。
- **要对象数** → KubeBrain 的 `count_index.keys`(便宜、现成)。
- **defrag**：安全 no-op，TiKV 自身 compaction/GC 由存储平台管理。
- **Alarm**：支持配额触发的 NOSPACE list/disarm，以及 root-only raw gRPC
  `Alarm(ACTIVATE, NOSPACE)` 故障注入；NONE mutation 和重复 disarm 为成功空操作，
  raw ACTIVATE 指定的非零 owner member ID 会原样持久化，未指定时使用 backend
  identity 的稳定 ID；owner 跨 serving replica 稳定，任一 endpoint 可使用该 owner
  解除。激活或解除提交结果不确定时服务端会独立回读确认；解除后若不同 owner 已重新
  激活，旧 alarm 的解除返回成功但 `quota.nospace` 保持 1，调用方应重新执行
  `alarm list`。首次启动创建 quota usage 或因存量超额自动激活 alarm 的提交结果不确定
  时，同样会在 readiness 前独立回读确认。错误 owner 不会解除 tenant alarm。
  CORRUPT 仍无对应语义。

active NOSPACE 是容量保护状态，不是进程不可服务：`/ready`、`/readyz` 应继续通过，
读和释放容量的删除操作仍可用；`etcdctl endpoint health` 的线性化 proposal 会按 etcd
契约返回 `Active Alarm(s): NOSPACE` 和 unhealthy。告警系统应以 `quota.nospace`/
`alarm list` 区分该状态，不能把 endpoint health 的这一结果直接等同于 Pod 未就绪。

变更 `--quota-backend-bytes` 时，同一 keyspace 的所有 serving 副本必须使用一致值。
禁用 quota 会在 leadership readiness 前把持久 tracking marker 置为 dirty；重新启用
会先扫描 live key 重建 usage，再把 usage 与 clean marker 原子提交。重建失败或滚动期间
启用/禁用配置混用时，Status/写入返回 quota uninitialized 并保持 fail closed；此时应
完成一致配置 rollout 并观察 `quota.initialize.err`，不能手工修改内部 marker 或 usage。
