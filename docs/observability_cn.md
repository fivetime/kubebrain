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
| `etcd_server_request_duration_seconds` histogram(labels: `type`,`success`) | etcd upstream 兼容的请求端到端耗时，覆盖 KV、Compact、LeaseGrant/Revoke、内部 LeaseCheckpoint、Alarm、Authenticate 与 Auth 管理类型；用于复用 etcd dashboard/runbook，并按 `success=false` 定位失败请求延迟。 |
| `etcd_network_known_peers{Local,Remote}` gauge | 本 KubeBrain 数据面 Pod 从启动期静态 `--initial-cluster` 看到的成员集合；Local/Remote 均为非零小写十六进制 member ID，每对值固定为 1。每个 Ready Pod 必须看到 StatefulSet desired 数量的 Remote，Local 在 Ready Pods 间唯一；不包含 TiKV/PD 节点。配置不可变，因此 retained gauge 合法，Pod UID 更替淘汰旧配置。 |
| `etcd_server_id{server_id}` gauge | 当前 KubeBrain Pod 的 upstream-compatible member identity，值固定为 1、label 为非零 canonical 小写十六进制。每个 Ready Pod 恰有一条，且 `server_id` 必须双向等于同 Pod `known_peers.Local`；该 ID 同时用于 Maintenance Status header/member 响应。 |
| `health_checkpoint_fallback` counter(labels: `check`) | HTTP health/livez/readyz 的 live TiKV 读因 DeadlineExceeded/Unavailable 转入 GC-protected checkpoint 的次数；`check` 固定为 `alarm`、`serializable_read`、`data_corruption`。增量表示有界陈旧降级服务，不表示请求失败，也不能证明 PD/TiKV 已恢复。 |
| `storage_batch_count`(op=`write_batch`)histogram | 存储层批量写延迟(TiKV TSO+raft)。 |
| `leader_revision` gauge | 当前 leader 的 revision(是否推进)。leader 身份见 info 端口 `/election`。 |
| `count_index.keys` / `count_index.overflowed` gauge | leader 活跃 key 数与索引是否超过 `--count-index-max-keys`；overflow 后 List/count 回退 TiKV 全扫，decoded-boundary RangeStream 改走本地有界外排，延迟均会明显恶化。followers 的 keys=0 是正常值。 |
| `backend.range_stream.decoded_spill` counter；`backend.range_stream.decoded_spill_{bytes,keys}` gauge；`backend.range_stream.decoded_spill_latency_seconds` histogram | decoded-boundary RangeStream 无法使用内存排序 index 时的外排次数、最近一次最终有序 run 大小/观察键数和端到端外排耗时。正常应接近 0；持续增长先检查 count-index overflow/rebuild 和 `TMPDIR` emptyDir 容量。每副本只允许一个外排，等待会计入 RPC 延迟。 |
| `count_index.rebuild.err` counter | leader 切换时索引快照重建失败次数；非零表示 CountOnly 暂时回退 TiKV 全扫。 |
| `watch.collector.stalled` / `watch.collector.skipped_revision` counter | 事件收集器 stall/自愈跳过 —— 每个 backend 初始化为 0；非 0=有 writer 死在 deal↔notify 之间。 |
| `revision.durable.persist_err` counter | durable user-revision watermark 后台/强制持久化失败次数；每个 backend 初始化为 0。非零时后台路径让离线 serializable reader 暂留旧安全快照，强制路径会阻塞 collector 连续推进直至成功。 |
| `watch.event_log.corruption{kind}` / `watch.event_log.corrupt_alarm_failed` counter | 仅在 trusted window 内重新检查 cleanup/compaction watermark 后仍确认的 event-log 损坏，以及随后持久化 CORRUPT alarm 失败；`kind` 固定为 `malformed`、`witness_mismatch`、`incomplete`、`invalid_object`。每个 backend 初始化四个 kind 和 alarm-failure 零值，任一增量均为 critical。 |
| `alarm.corrupt_active` gauge | 当前副本最近一次成功读取共享 TiKV alarm 集合时是否存在任一 CORRUPT owner；RPC server 创建时发布 0，随后每 15 秒刷新为精确 0/1。任一 1 都表示受保护写已被 DataLoss fence。 |
| `storage.gc.enabled` / `storage.gc.{driver_started,last_success}_timestamp_seconds` gauge；`storage.gc.err` counter | 裸 PD/TiKV 的内建 gc_worker capability、启动/最近真实成功时间和 safepoint 推进失败。每个 backend 先发布 enabled=0、两个 timestamp=0、error=0，具备 GarbageCollector 且 lifetime>0 后记录 driver start；只有 leader 的成功 GC 调用更新 last-success。默认 10 分钟周期，20 分钟无任一 Ready leader 成功即 stalled。 |
| `txn.uncertain.resolution{outcome}` counter | TiKV commit 结果不确定后的 durable event-marker resolver；固定 outcome 为 `retry`、`committed`、`not_committed`、`witness_corrupt`、`corrupt_alarm_failed`，每个 backend 初始化五条零值。前两类终态和 retry 是 warning 诊断，witness contradiction/alarm failure 为 critical。 |
| `etcd_debugging.auth.revision` gauge / `auth.revision.refresh.err` counter | 每秒从共享 `auth/config` 读取的持久 auth revision 及其纯 telemetry 刷新失败；error counter 在 RPC server 创建时初始化为 0。Ready 副本应收敛到同一正整数 revision；刷新失败保留旧 gauge，不等同于请求授权失败。 |
| `etcd_debugging.mvcc.{current_revision,compact_revision}` gauge / `mvcc.compact_revision.refresh.err` counter | 每副本本地连续 current revision 与每秒从 TiKV 读取的共享 compact watermark。compact 必须为非负精确整数且不超过同 Pod current；共享 compact 应在两分钟内收敛，follower current 可短暂不同。refresh-error 在 server 创建时初始化为 0。 |
| `os.fd.{used,limit}` / `fd.refresh.last_success_timestamp_seconds{type}` gauge；`fd.refresh.err{type}` counter | 进程文件描述符占用/上限及 `used|limit` 各自最近一次真实读取成功时间。server 创建时为两类 error/timestamp 发布权威 0，随后立即读取并每 10 分钟刷新；scrape timestamp 只证明 exporter 仍提供 retained gauge，20 分钟未推进 last-success 才表示刷新停滞。used 不得超过 limit，持续超过 80% 告警。 |
| `serializable.checkpoint.available` / `serializable.checkpoint.remaining_seconds` gauge；`serializable.checkpoint.refresh_err` counter | 每副本受 PD service GC safepoint 保护的离线 serializable checkpoint 是否可用及本地安全窗剩余秒数；available 应恒为 1，remaining 默认每秒回到约 150。refresh-error counter 启动时发布权威零值；非零或 remaining 持续降至 0 表示该副本将在 PD 隔离时对 Range/read-only Txn/RangeStream fail closed。 |
| `lease.orphan_sweep.{key_deleted,legacy_key_deleted,record_reclaimed,err}` counter | 孤儿 lease 清扫活动；`legacy_key_deleted` 表示依靠同 revision ownership witness 回收升级前 v1 leased value —— 正常均应极低。 |
| `lease.legacy_migration_seal.err` counter | legacy user-MVCC lease source 已清空但 internal migration seal 写入失败次数；非零时 loader 仍保持兼容扫描，需检查 leadership/CORRUPT/TiKV fence。 |
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
| `etcd_debugging_mvcc_watch_stream_total` / `etcd_debugging_mvcc_watcher_total` / `etcd_debugging_mvcc_slow_watcher_total` gauge | 当前 member 的公开 Watch gRPC stream、backend 逻辑订阅和处于 ring catch-up 的订阅数。stream 可暂时无 watch，一个 stream 也可 multiplex 多个 watch，因此前两者不要求相等；slow 必须不超过 watcher。production 每秒刷新，任一 Pod 超过 8000 streams/watchers 或 catch-up 持续 5 分钟告警。 |
| `etcd_debugging_mvcc_pending_events_total` gauge / `etcd_debugging_mvcc_events_total` counter | etcd upstream 兼容的 Watch event backlog / delivered event 观测；pending 统计本 member 已转换但尚未交给 watch send loop 的本地 backend WatchResult 事件数，events 统计成功交付到 watch response 层的事件数。 |
| `etcd_mvcc_hash_duration_seconds` / `etcd_mvcc_hash_rev_duration_seconds` histogram | etcd upstream 兼容的 Maintenance Hash/HashKV 成功计算耗时；用于定位诊断 hash 受 keyspace、revision/compaction 查找或 TiKV 读路径影响。 |
| `etcd_debugging_mvcc_db_compaction_last` gauge / `etcd_debugging_mvcc_db_compaction_total_duration_milliseconds` histogram / `etcd_debugging_mvcc_db_compaction_pause_duration_milliseconds` histogram / `etcd_debugging_mvcc_index_compaction_pause_duration_milliseconds` histogram / `etcd_debugging_mvcc_db_compaction_keys_total` counter | etcd upstream 兼容的物理 compaction 观测；KubeBrain 将 shared-storage version GC 总耗时映射到 db compaction total，将每次物理删除 batch/fallback 写事务耗时映射到 db compaction pause，将 count-index 裁剪耗时映射到 index compaction pause，并按 full/incremental scan 实际成功删除的物理 MVCC key 累加 keys total。只读扫描时间不算 db pause。 |
| `etcd_debugging_mvcc_total_put_size_in_bytes` gauge | etcd upstream 兼容的本 member 成功 Put key/value 字节累计值；KubeBrain 在 public Put 和成功执行的 Txn Put op 后按实际 key+value 长度累加。 |

## Health checkpoint fallback 告警处置

`KubeBrainHealthCheckpointFallback` 按 `check` 报告最近 10 分钟实际进入 protected checkpoint 的 health
检查。收到告警后不要因为 `/health?serializable=true`、`/livez` 或公开 gRPC Health 仍成功就关闭事件：这些接口
只证明进程可接流且仍有一份 GC-protected 有界陈旧状态。

`KubeBrainHealthCheckpointFallbackMetricsMissing` 按当前 Ready Pod UID 动态计算期望值，要求每个 Ready
副本各发布三条固定 check 的新鲜 current 与 10 分钟 increase series，因此正常扩缩容不会因固定三副本
假设而误报。current counter 必须是 `[0,2^53]` 内精确整数；Prometheus 外推 increase 可以是分数，但
必须有限且在 `[0,2^53]` 内。invalid-value recording 缺失/非零同样告警。该告警持续
5 分钟时，先排除混合版本滚动、ServiceMonitor 缺少 Pod UID relabel、旧 Pod scrape target 残留和 registry 初始化
缺失；在恢复 `3 × Ready 副本数` 条当前 Pod UID 对应的 series 前，不得把 fallback 告警静默解释为
“没有降级事件”。

1. 先检查 `/readyz?verbose` 和一条有 deadline 的真实线性 Range/事务探针；linearizable read 失败表示实例不可接收
   需要最新状态的流量。
2. 检查 PD leader/quorum、TiKV Region leader/peer 与 Store 状态，并对照
   `serializable_checkpoint_available`、`serializable_checkpoint_remaining_seconds` 和
   `serializable_checkpoint_refresh_err`。不要手工延长 remaining time 或删除 service GC safepoint。
3. 只有在线性探针恢复、PD/TiKV 拓扑健康且 fallback counter 的新增速率归零后才解除降级事件。counter 是单调累计值，
   不要求其回到 0。
| `etcd_server_snapshot_apply_in_progress_total` gauge | etcd upstream 兼容的 raft snapshot apply 状态；KubeBrain 不运行 etcd raft snapshot apply，固定为 0，TiKV/PD snapshot 应看存储层指标。 |
| `etcd_server_heartbeat_send_failures_total` counter | etcd upstream 兼容的 raft leader heartbeat 发送失败计数；KubeBrain 不运行 etcd raft transport，固定为 0，TiKV/PD heartbeat 应看存储层指标。 |
| `etcd_server_proposals_committed_total` / `etcd_server_proposals_applied_total` / `etcd_server_proposals_pending` gauge，`etcd_server_proposals_failed_total` counter | etcd upstream 兼容的 raft proposal 状态；KubeBrain 不运行 etcd raft proposal pipeline，四者固定为 0。不得解释为 MVCC revision、public write RPC 或 TiKV transaction；底层共识使用 TiKV/PD 原生指标。 |
| `etcd_server_learner_promote_successes` / `etcd_server_learner_promote_failures{Reason}` counter | etcd upstream 兼容的 learner promotion 结果；DBaaS 控制面拥有成员变更，因此数据面成功数固定为 0。实际 `MemberPromote` 终止错误按 upstream `Reason=err.Error()` 计入 failures；不要把 TiKV learner 或控制面扩缩容结果混入。 |
| `etcd_server_slow_read_indexes_total` / `etcd_server_read_indexes_failed_total` counters | etcd upstream 兼容的线性读 barrier 结果。KubeBrain 在中央 `SyncReadRevision` 路径把 deadline/transport timeout 计入 slow，把 stale leader、leader change 或其他终止错误计入 failed；客户端主动取消和成功 barrier 不增加。该边界覆盖 Range、只读 Txn、Watch、Maintenance 等所有线性化调用者。 |
| `etcd_disk_backend_commit_duration_seconds` histogram | etcd upstream 兼容的 backend 原子提交耗时。KubeBrain 观察每次 TiKV/Badger batch commit attempt，以及 direct delete commit；成功、确定失败和不确定结果均有样本。bucket 与 upstream 保持 1ms 起、2 倍递增、共 14 桶。进程启动时只注册 count=0 family，不用 `Observe(0)` 伪造提交。该 family 独立于可选的 `--enable-storage-metrics`，后者只控制内部 `storage.*` 详细序列。 |
| `etcd_debugging_disk_backend_commit_{rebalance,spill,write}_duration_seconds` histograms | upstream bbolt commit 的三个进程内部阶段。KubeBrain 的等价持久化边界是 TiKV/Badger 原子 commit，无法诚实拆分为 bbolt rebalance/spill/write；因此三个 family 均按 upstream 14 个 1ms–8.192s bucket 注册，但 count 固定为 0。不得把 distributed transaction 延迟重复计入这些 bbolt-only family。 |
| `etcd_disk_backend_snapshot_duration_seconds` histogram | etcd upstream 兼容的 backend snapshot 生命周期耗时。KubeBrain 只在实际执行导出的 leader 上观察临时 bbolt 构建到最终 digest frame 发送完成/失败的总时长；follower 代理不重复计数。bucket 与 upstream 保持 10ms 起、2 倍递增、共 17 桶；启动时只注册 count=0 family。 |
| `etcd_disk_backend_defrag_duration_seconds` histogram / `etcd_disk_defrag_inflight` gauge | etcd upstream 兼容的 embedded backend defrag 状态。KubeBrain 不运行 bbolt defrag，TiKV/PD compaction 属于独立存储层，因此 duration count 固定为 0、inflight 固定为 0；兼容 Defragment RPC 的 no-op 成功也不伪造物理整理样本。 |
| `etcd_server_healthcheck` gauge(labels: `type`,`name`) | etcd 兼容的 `/livez`、`/readyz` 分项状态；1=最近一次成功，0=最近一次失败。 |
| `etcd_server_healthchecks_total` counter(labels: `type`,`name`,`status`) | 分项检查累计结果；用 `rate(...{status="error"}[5m])` 区分后端不可读与无 leader。传统 `/health` 同时提供 `etcd_server_health_success`/`failures`。 |

## 推荐告警(方向,阈值按环境调)

- **leader 频繁切换 / 抖动**:`rate(etcd_server_healthchecks_total{type="readyz",name="linearizable_read",status="error"}[5m]) > 0` 持续出现，或 `write.fence.reject` 持续增长，或 `/election` leader 地址频繁变。正常换主不应导致 Pod 重启；重启率 > 0 是独立的进程稳定性告警。每个副本在 campaign 前初始化 leadership-lost、通用 initialization-error、incompatible-witness、invalid-alarm-metadata 四类 counter；production 从 60 秒内样本生成 Ready Pod UID 级 current/10 分钟 increase recording，要求八类来源完整且值合法。lost counter 先按动态前任 leader `addr` 去重再按 Pod 求和，不能吞掉不同地址的多次丢主。通用 initialization alert 覆盖 lease/event/count/checkpoint 等未由两个 durable-metadata 子类单独分类的失败。缺失、陈旧或非法 telemetry 不得掩盖抖动或拒绝领导。→ 见 [[failover_tuning_cn.md]](租约调优)。
  acquired leader 在 SERVING 前的 compact resume、quota init、lease reload、event-log watermark、checkpoint
  protection 五阶段另使用固定 `stage` counter。所有副本 campaign 前各初始化五条零值；failure 告警不关联
  Ready UID，因为阻塞中的 leader 会主动 NotReady。completeness 将 60 秒新鲜 current/increase 总数与
  `5 × StatefulSet desired replicas` 对账并验证值域，缺失/陈旧/非法 telemetry 不能隐藏 pre-serving 重试。
- **共享存储不可读**:`etcd_server_healthcheck{type="livez",name="serializable_read"} == 0`，或对应 error counter 持续增长。它不依赖 KubeBrain leader，可直接指向 TiKV/PD、网络或租户 keyspace 读路径。
- **写延迟过高**:`write.latency` p99 持续 > 你的 lease-sensitive 控制器续期窗口的一半(默认 controller-manager ~15s → 阈值 ~7s;满载 TiKV 单 region 热点会推高)。
- **PD/TiKV 持久化延迟越界**：生产规则分别对
  `etcd_disk_wal_fsync_duration_seconds_bucket`、
  `tikv_raftstore_store_write_raftdb_duration_seconds_bucket` 和
  `tikv_raftstore_store_write_kvdb_duration_seconds_bucket` 计算每个实例的 5 分钟 p99；任一持续 1 分钟
  超过 1 秒即 critical。PD 官方告警本身也把 WAL fsync p99 >1s 视为 critical；对 KubeBrain 而言，
  该延迟会先拖慢 PD Raft heartbeat/leader lease，再让 TiKV TSO 和短 etcd lease 续期整体失去进展，
  即使 Pod Ready、store Up、Region leader-missing 仍为零也不能视为健康。
  `KubeBrainStorageLatencyMetricsMissing` 将按 `instance` 去重的 PD WAL 和 TiKV RaftDB/KVDB histogram count
  series 分别与当前 PD/TiKV StatefulSet 期望副本数精确对账；任一扩容副本缺失 family，或期望 recording
  series 缺失，持续 5 分钟即 warning。`KubeBrainStorageLatencyMetricsStale` 另以
  `time()-timestamp(...)` 检查三个 family 的最旧样本，超过 60 秒持续 1 分钟即 warning，避免 Prometheus
  lookback 仍返回已停止抓取的旧 series。缺失或陈旧都禁止被误判成低延迟。阈值是发布下限，不是
  云盘选型承诺；容量、IOPS 与 tail-latency SLO 仍需按套餐压测并收紧。
  正式发布入口会调用 `hack/production/validate-storage-latency-slo.sh`，以相同表达式对三个 PD/TiKV
  副本执行即时 fail-closed 检查，同时拒绝超过可配置 sample age 的陈旧 telemetry；这补足 Prometheus `for` 窗口尚未进入 firing 时的发布前拒绝，持续告警
  仍负责发布后的运行时保护。
- **扩容副本资源异常**：存储 PVC 低水位、容器内存/CPU throttling、网络错误与丢包规则按
  StatefulSet 数字 ordinal 匹配全部 `kubebrain-N`、`kb-pd-N`、`kb-tikv-N` 及对应 PVC，不局限于
  默认 0–2。因此扩容后的新副本也必须纳入运行时告警；恢复作业等非数字后缀 Pod 仍被排除。
  `KubeBrainStorageVolumeMetricsMissing`、`KubeBrainResourceMetricsMissing` 和
  `KubeBrainNetworkMetricsMissing` 不只检查默认最低 6/9 条：容器指标与去重后的当前 StatefulSet
  期望副本数精确对账，PVC requested-storage source 与去重后的当前匹配 PVC 对象数精确对账，kubelet
  capacity/available 则只与当前活跃存储副本数对账。缩容保留卷即使已卸载，在删除前仍纳入 provisioned storage 监控/计费；不伪造其 used bytes。扩容后 12 个容器只有 11 条指标也会 fail closed。任一期/source recording series 缺失也直接告警。
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
  `watch_revision_lag` 使用本 Pod 已实际入 ring 的最高 revision 减去 collector 连续游标；入队和 collector
  每次推进都会刷新，因此写高峰后即使没有后续写入，也必须随消费下降并最终回到 0。若 gauge 保持高位，
  表示真实积压而不是最后一次 enqueue 的历史快照。每个副本启动时立即发布，并每 15 秒刷新一次当前值；
  production recording 只保留 60 秒内样本并按 Pod UID 去重，要求与当前 Ready UID 集合精确相等。
  `KubeBrainRevisionLagMetricsMissing` 表示不能再信任 backlog 零值；结合 stale-drop/full/skipped-revision
  counter 定位。
  `watch_event_buffer_stale_drop` 与 `watch_event_buffer_full` 也在每个 backend 创建时发布权威零值；
  production 仅消费 60 秒内、按 Ready Pod UID 去重的 current 与 5 分钟 rate recording。两类 current
  必须是 `[0,2^53]` 内精确整数，外推 rate 可为分数但必须有限且同范围；四类来源必须完整覆盖当前
  Ready UID。`KubeBrainWatchEventBufferMetricsMissing` fail closed，缺失、陈旧或非法 telemetry 不能解释为
  没有丢事件或没有 buffer overflow；事件告警也只消费这些新鲜 recording。
  collector 的 stalled/skipped-revision counter 同样在 backend 创建时初始化为权威零值，并生成 60 秒
  新鲜的 Ready Pod UID 级 current/10 分钟 increase recording。current 必须是 `[0,2^53]` 内精确整数，
  外推 increase 可为分数但必须有限且同范围；四类来源必须完整。stall 立即 warning，超过有界 publication
  deadline 后跳过 abandoned revision 立即 critical；`KubeBrainWatchCollectorMetricsMissing` 拒绝把缺失、
  陈旧或非法 telemetry 当成每个已分配 revision 都已发布或安全解析的证据。
  durable revision persist-error counter 也生成 60 秒新鲜的 Ready Pod UID 级 current/10 分钟 increase；
  current 必须是 `[0,2^53]` 内精确整数，外推 increase 可为分数但必须有限且同范围，两类来源必须完整。
  任一增量立即 critical；`KubeBrainDurableRevisionPersistenceMetricsMissing` 拒绝用缺失、陈旧或非法 telemetry
  证明 watermark、离线安全快照或 collector continuity 健康。
  event-log integrity 不直接告警会在竞态排除前递增的 legacy malformed/incomplete counter；canonical
  `watch.event_log.corruption{kind}` 仅在重新读取 cleanup 与 compact watermark 后仍确认 trusted-window 损坏
  才递增。四个固定 kind 与 CORRUPT-alarm failure 分别生成 60 秒新鲜的 Ready Pod UID 级 current/10 分钟
  increase，要求 `4×Ready` 与 `1×Ready` 精确覆盖；current 为 `[0,2^53]` 精确整数，increase 有限且同范围。
  确认损坏或 alarm 持久化失败均立即 critical，缺失、陈旧或非法 telemetry 不能证明 event-log 完整或安全
  fence 已持久化。
- **离线 serializable checkpoint 不可用/即将过期**：available/revision/remaining-seconds series 任一未覆盖全部当前 Ready Pod、`min(available) < 1`、`min(revision) <= 0`、`min(remaining_seconds) < 60`，或 invalid-value recording 缺失/非零，持续 30s 即告警。三类 raw gauge 先仅从 60 秒内样本按 `namespace,pod,uid` 去重为 current recording，再与同身份聚合去重的 kube-state-metrics Ready Pod 相交，三者的期望基数都动态等于当前 Ready Pod UID 数。这既避免滚动更新后同名旧 Pod 的残留 series 在 Prometheus lookback 窗口内制造重复副本，也允许正常扩缩容并兼容多副本 kube-state-metrics。available 必须精确为 0/1，revision 必须是 `(0,2^53]` 内精确整数，remaining seconds 必须是 `[0,2^53]` 内非负精确整数；缺少 UID、Ready 状态、Ready Pod 指标、fresh recording 或合法值都 fail closed。`serializable_checkpoint_revision` 暴露各副本当前受保护 revision，并在 checkpoint 不可用时归零，可用于判断隔离前写入是否已被固定快照覆盖。结合 `increase(serializable_checkpoint_refresh_err[10m])` 判断是 scrape、PD safepoint、Region/store directory warmup、metadata 还是刷新链路失败。正常 PD quorum 下最新读仍可工作，但该副本已失去或将在一分钟内失去有界 PD 隔离读能力。
  refresh-error counter 另生成 60 秒新鲜的 current 与 10 分钟 increase recording，并要求两者各精确覆盖
  当前 Ready Pod UID。current 必须是 `[0,2^53]` 内精确整数；外推 increase 可为分数，但必须有限且在
  同一范围内。缺失、陈旧或非法 counter 不能解释为零 refresh failure。
  正式发布入口还会调用 `validate-storage-latency-slo.sh` 对同一 available/revision/remaining 数量、下限和新鲜度执行即时 fail-closed 检查，不等待告警 `for` 窗口。
- **watch send loop 拥塞**:`histogram_quantile(0.99, rate(etcd_debugging_server_watch_send_loop_watch_stream_duration_seconds_bucket[5m]))` 或 control/progress send-loop p99 持续升高。若这些指标高而 TiKV/collector 正常，优先查 gRPC 流控、客户端消费速度和 apiserver watch cache 初始化并发。
- **版本膨胀**:`count_index.keys` 长期单调上涨且无压缩回落 → 检查 apiserver 压缩循环是否正常(KubeBrain 自身不自动压缩)。
- **count index 退化**：只对 KSM Ready 与 `count_index_overflowed` 原始样本均不超过 60 秒的
  `(namespace,pod,uid)` 计算 `max(kubebrain_dbaas:count_index_overflowed:max_by_pod) > 0`，持续 1m；
  每个当前 Ready UID 都必须有一条新鲜 overflow gauge，否则 `KubeBrainCountIndexMetricsMissing`
  在 2m 后告警，不能把缺失/停止抓取解释成未溢出。另监控
  `count_index_rebuild_err`：每个副本在参与 leader election 前初始化为权威零值，production 从 60 秒内
  样本生成 current 与 10 分钟 increase recording，并要求两者分别覆盖当前 Ready Pod UID。current 必须是
  `[0,2^53]` 内精确整数，外推 increase 可为分数但必须有限且同范围；缺失、陈旧或非法值不能解释为零
  rebuild failure。overflow 时应同时提高实例内存规格和
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
  NOSPACE、90% 水位、当前 Ready Pod UID 集合的 series 缺失/不一致以及共享状态读取失败。
  每个副本在首次 refresh 前初始化 `quota_refresh_err=0`；production 从 60 秒内样本生成 current 与
  10 分钟 increase recording，并要求两者分别覆盖当前 Ready Pod UID。current 必须是 `[0,2^53]` 内
  精确整数，外推 increase 可为分数但必须有限且在同一范围；`KubeBrainQuotaRefreshMetricsMissing`
  拒绝缺失、陈旧或非法 counter，不能把 family 缺失当作零读取失败。
  NOSPACE、UsageHigh 和 MetricsInconsistent 三条规则都排除已终止 Pod 的陈旧 UID 样本，completeness
  期望值随 Ready 副本数动态变化。换主或 mutation
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
  CORRUPT 使用 TiKV 共享 member/generation metadata、写提交 generation fence 与显式校验后解除语义；
  任一 owner 激活都会阻断受保护写并让 data-corruption readiness 失败。
  每个副本在共享 alarm refresh 启动前初始化 `alarm_refresh_err=0`；production 从 60 秒内样本生成
  current 与 10 分钟 increase recording，并要求分别覆盖当前 Ready Pod UID。current 必须是
  `[0,2^53]` 内精确整数，外推 increase 可为分数但必须有限且同范围。`KubeBrainAlarmRefreshFailures`
  报告实际 TiKV metadata 读取失败，`KubeBrainAlarmRefreshMetricsMissing` 拒绝缺失、陈旧或非法 counter。
  刷新失败时 exporter 保留上次 alarm gauge 快照，不能把该 stale snapshot 当作当前共享状态。
  `alarm_corrupt_active` 在 RPC server 创建时发布权威 0，每次完整 refresh 成功后按共享 CORRUPT owner
  集合刷新为精确 0/1。production 只接受 60 秒内且属于当前 Ready Pod UID 的样本；任一 1 立即 critical，
  覆盖缺口、非 0/1 或副本分歧持续 1 分钟也 critical。解除前必须保留证据并通过 AlarmDeactivate 的
  transaction-witness/alarm-generation 校验，不能直接删除内部 alarm key。

active NOSPACE 是容量保护状态，不是进程不可服务：`/ready`、`/readyz` 应继续通过，
读和释放容量的删除操作仍可用；`etcdctl endpoint health` 的线性化 proposal 会按 etcd
契约返回 `Active Alarm(s): NOSPACE` 和 unhealthy。告警系统应以 `quota.nospace`/
`alarm list` 区分该状态，不能把 endpoint health 的这一结果直接等同于 Pod 未就绪。

变更 `--quota-backend-bytes` 时，同一 keyspace 的所有 serving 副本必须使用一致值。
禁用 quota 会在 leadership readiness 前把持久 tracking marker 置为 dirty；重新启用
会先扫描 live key 重建 usage，再把 usage 与 clean marker 原子提交。重建失败或滚动期间
启用/禁用配置混用时，Status/写入返回 quota uninitialized 并保持 fail closed；此时应
完成一致配置 rollout 并观察 `quota.initialize.err`，不能手工修改内部 marker 或 usage。
