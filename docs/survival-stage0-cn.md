# apiserver 生存性 Stage-0:零代码配置包(kubeadm 1.36.2+)

**目标**:不改一行 k8s 代码,让原味 kube-apiserver(kubeadm ≥1.36.2)+ KubeBrain 在百万~千万级对象规模下稳定活着。每条配置都有实测出处(scale-lab 千万级战役、2100 万对象测试、1.37.0-alpha.3 冷启动 A/B)。

适用镜像:KubeBrain ≥ `d4f83fc`(含 `/version` 双口、增量物理 GC、progress 即时扇出;`--compatible-with-etcd` 已默认 true)。

---

## 1. kubeadm-config 样例(实测配方)

```yaml
apiVersion: kubeadm.k8s.io/v1beta4
kind: ClusterConfiguration
kubernetesVersion: v1.36.2
etcd:
  external:
    endpoints:            # KubeBrain 三副本 client 口
      - http://10.32.32.101:3379
      - http://10.32.32.102:3379
      - http://10.32.32.103:3379
    # 1.37 起可用 httpEndpoints 分离 HTTP 预检口(KubeBrain 的 /version 双口都有)
apiServer:
  extraArgs:
    # 千万级必关:1.34+ Beta 默认开,周期性全量 LIST 对账 watch cache
    # (冷启动实测:它贡献了持续的分页穿透扫描)
    - name: feature-gates
      value: DetectCacheInconsistency=false
    # 冷启动 informer 洪水下放大在飞请求额度
    - name: max-requests-inflight
      value: "3000"
    - name: max-mutating-requests-inflight
      value: "1000"
    # ⚠️ 不要设置 etcd-compaction-interval=0:apiserver 是 KubeBrain
    # compaction 的唯一驱动者,禁掉=MVCC 版本无限堆积、读持续劣化。
    # 保留默认 5m 即可(不写这个 flag)。
controllerManager:
  extraArgs:
    # nodeipam 的 cluster-cidr 只支持一个 /16 内的分配位图:10 万节点
    # (maxPods 1024 → /22/节点)远超上限,必须关掉 kcm 的 CIDR 分配,
    # 交给 CNI 自己管(实测发现:nodeipam /16 上限)
    - name: allocate-node-cidrs
      value: "false"
```

**kubeadm 操作纪律**(实测踩坑):
- 凡 `kubeadm init phase upload-certs` **必带 `--config`** ——不带时默认"本地 etcd"模式会把 Secret 写成 `etcd-ca.*`,盖掉 `external-etcd-*`,后续加控制面报 `external-etcd-ca.crt` 找不到。
- 命令一行写完,续行 `\` 后不得有空格(否则 "unknown command")。

## 2. apiserver 的三条铁律

| 规则 | 理由(实测) |
|---|---|
| **watch-cache 保持开启**(默认;勿设 `--watch-cache=false`) | 关掉 = 每个 LIST 都穿透存储,千万级下是自杀式反向操作 |
| **`DetectCacheInconsistency=false`** | 默认开;千万级下常驻全量对账扫描 |
| **`GOMEMLIMIT` 按机器内存设置**(static pod env,如物理内存的 80%) | 1500 万对象 ≈ 100G RSS、2100 万 ≈ 180-216G(两轮实测);无上限的 Go heap 在内存边缘触发 OOM→重启→全量重灌的死亡螺旋 |

1.37+ 追加:`EtcdRangeStream` 正式版默认开(alpha 需显式 `=true`),watch-cache 初始化走流式,实测 init 快 2.8×;北极星规模建议再开 `ConsistentListFromCacheSkipTimeoutFallback=true`(429 替代超时后的全量 LIST 穿透,对存储纯减压)。

### ⚠️ 2GiB 硬墙:单资源类型总量决定 1.36 能否冷启动(实测坐实)

**≤1.36 的 apiserver 对"单个资源类型全量 LIST 响应 >2GiB"的数据集永远无法冷启动**——watch-cache 初始化对 etcd 发的是不分页全量 Range,响应超过 gRPC/protobuf 的 int32 上限(2147483647 字节)时服务端拒发,apiserver 每 ~40s 重试一次,**无限循环,永不就绪**。实测:490 万 deployment(约 600B/个)= 2 949 012 631 字节,k3s v1.36.2 节点 NotReady 挂死 18h+。

- 这是协议级的墙,与存储后端无关:真 etcd 同样中招(`server/etcdserver/api/v3rpc/grpc.go` 将 `MaxSendMsgSize` 设为 `math.MaxInt32`),KubeBrain 行为与 etcd 一致。
- 容量公式:**每种资源 `对象数 × 平均编码大小 < 2GiB`**,超线即必须 1.37+`EtcdRangeStream`(流式分块,无单消息上限)。这是"千万级对象必须 1.37"的最硬理由——不是性能问题,是可用性问题。
- 注意坏状态可以潜伏:apiserver 在线时增量写入不触发全量 LIST,数据涨过线后**下一次重启**才爆雷。上线前用 `etcdctl get <prefix> --prefix --count-only` × 抽样对象大小自查每个大类型。

## 3. KubeBrain 部署参数(podman/quadlet 样例)

```
kube-brain
  --pd-addrs=<pd1>:2379,<pd2>:2379,<pd3>:2379
  --advertise-host=${NODE_IP}          # 多网卡必设,否则 identity 瞎选网卡
  --port=3379 --peer-port=3380 --info-port=8080
  --enable-count-index=true
  --count-index-max-keys=30000000      # ≥ 预期对象总数,否则 index 自动停用
  --storage-gc-lifetime=10m            # 裸 PD+TiKV 必须:唯一推 GC safepoint 的组件
  --auto-compaction-retention-revisions=50000000   # 安全网:apiserver compaction 断链时兜底
  # --watch-progress-notify-interval 保持默认 1s;>2.5s 会被启动校验直接拒绝
  # --compatible-with-etcd 已默认 true,无需显式
```

## 4. 死亡螺旋的四层与各层断点

历史故障模式:`repair 60s Fatalf → kubelet 重启放大 → informer 全量 LIST 蹚墓碑 → compaction 死锁`。逐层现状:

1. **service-ip-repair 60s 死线**:唯一的**零代码硬墙**。实测边界:**50 万 services 可过、100 万必死**(1.37.0-alpha.3 原样复现,62s Fatalf)。规划 service 总量,超界需 fork 补丁(见 `upstream-pr-drafts.md`)或等上游。
2. **重启放大**:GOMEMLIMIT 防 OOM 触发源;kubelet static-pod backoff 不可调,减少重启原因是唯一解。
3. **LIST 蹚墓碑**:KubeBrain #62 修复(物理 GC 全 keyspace)+ 增量 GC 已结构性消除墓碑堆积;监控见下。
4. **compaction 死锁**:#44(写队头)/#66(批量删)/增量 GC 已解决;`--auto-compaction-retention-revisions` 是最后防线。

## 4.5 大规模清退(百万级对象删除)的三堵墙(#74 实测)

| 墙 | 实测数字 | 对策 |
|---|---|---|
| **kcm 默认 `--kube-api-qps=20`** | 100 万 ns 级联 0.9 ns/s ≈ 12 天 | `--kube-api-qps=1000 --kube-api-burst=1500`(提速后 KubeBrain 稳定承接 835 req/s) |
| **ns 级联的固有成本 ~243 个 API 调用/ns**(发现+逐 GVR List/DeleteCollection+复核+条件更新) | QPS 拉满也是 3.4 ns/s ≈ 80h/百万 ns | 百万 ns 规模别走 namespace 级联;按资源类型批量删(或直连存储清理),ns 对象最后删 |
| **k3s 专属:node 挂 `wrangler.cattle.io/node` finalizer** | 10 万 node 的 DELETE 全部秒回成功,但对象按 ~70/s 逐个消失(24min) | 预期内的滞后,勿重复删除;真 k8s 无此墙 |

另:孤儿 RS(owner 已删)在被 GC 收走前仍会补建 pod(GC ~20/s 赶不上 replicaset 控制器),先删 Deployment/RS 再删 pod,顺序不能反。

## 5. 上线前检查清单

```
[ ] curl http://<kb>:3379/version 与 :8080/version 都返回 {"etcdserver":"3.7.0",...}
[ ] KubeBrain 日志无 "--watch-progress-notify-interval ... too large" 启动拒绝
[ ] count-index cap ≥ 预期对象数
[ ] 裸 TiKV:确认 storage_gc_safepoint 指标在推进(或环境里有 TiDB 在推)
[ ] apiserver manifest 里有 DetectCacheInconsistency=false、无 etcd-compaction-interval=0、无 watch-cache=false
[ ] kcm allocate-node-cidrs 与 CNI 的分工已明确
[ ] service 总量规划 < 50 万(硬墙)
```

## 6. 监控指标(KubeBrain :8080/metrics)

| 指标 | 健康形态 | 异常含义 |
|---|---|---|
| `compact{...}` 计数 | 随写入持续增长 | 冻结 = 物理 GC 停摆(#62 症状) |
| `compact_physical_done_rev` vs `leader_revision` | 差值有界 | 持续拉大 = GC 追不上 |
| `backend_compact_incremental` / `_keys` / `_fallback` | 稳态以增量为主,keys=近期写入量级 | fallback 持续 = elog 窗口/规模问题 |
| `storage_gc_safepoint` | 持续推进 | 停 = TiKV MVCC 无限堆积,读劣化 |
| `read_range_stream` / `_latency` | 1.37+ 冷启动时增长 | — |
| `watch.progress.request` | 有流量 | — |
| ⚠️ `etcd_db_total_size_in_bytes` | **恒 0(有意)** | 依赖它的告警会静默,容量走 TiKV/PD 带外 |

磁盘容量判断:**不要用 `du tikv-20160`**(含 21G 固定占位文件 + raft 日志);看 `tikv_engine_size_bytes{db="kv"}`。
