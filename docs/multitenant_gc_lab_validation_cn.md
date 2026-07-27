# 多租户 GC 隔离 —— Lab 验证方案(cherry-pick 进 main 前的门禁)

对应改动:`fix(gc): scope PD GC service safepoint per keyspace (#76)`(dbaas `19cb507c`)。

## 为什么只验这一块

这个修复对**默认 `""` keyspace 逐字节不变**(仍是 `gc_worker` + 无限 TTL),而 30M 跑的就是单租户默认 keyspace——那条路径已被覆盖。**唯一没被现有测试/现场跑覆盖的是"命名 keyspace 的多租户 GC 行为"**:单测只覆盖了纯派生函数 `resolveGCService`,真正的 `UpdateServiceGCSafePoint` min/clamp、有限 TTL 过期都要**真 PD** 才能验。所以本方案只精准打三点:

1. **服务名 + TTL 正确**(live PD 上 `resolveGCService` 的产物);
2. **跨租户 min/clamp 不互删**(核心正确性);
3. **离场租户 TTL 自愈**(我引入的有限-TTL 取舍成立)。

外加一条回归护栏:**默认 keyspace 仍是 `gc_worker`/无限 TTL**。

## 环境

- **需要**:一套共享 PD/TiKV + 用**本修复二进制**跑的 ≥2 个命名 keyspace 租户。
- **推荐宿主**:`finding-multitenant-clustermesh` 那套「一 PD/TiKV 跑多 KubeBrain 租户」的 lab(10.32.32.x),现成拓扑直接改 keyspace/lifetime 即可。
- **构建**(本修复二进制):
  ```bash
  git checkout dbaas && bash build/build-tikv.sh   # → ./bin/kube-brain
  ```
- **观测主工具**:`pd-ctl`(集群全局真相,别只看单节点指标)
  ```bash
  pd-ctl -u http://<PD_IP>:2379 service-gc-safepoint
  ```
  输出形如:
  ```json
  {
    "service_gc_safe_point": [
      {"service_id": "gc_worker",            "expired_at": 9223372036854775807, "safe_point": 452...},
      {"service_id": "kubebrain-ks-tenant-a","expired_at": 1785...,             "safe_point": 452...},
      {"service_id": "kubebrain-ks-tenant-b","expired_at": 1785...,             "safe_point": 451...}
    ],
    "gc_safe_point": 451...      // 集群实际回收线 = 上面 safe_point 的 min
  }
  ```
  关键字段:`service_id`(=`resolveGCService` 产物)、`expired_at`(TTL 到期 unix 秒;无限 TTL≈`math.MaxInt64`)、`safe_point`(该租户地板 TSO)、顶层 `gc_safe_point`(clamp 后的集群线)。

> 提醒:GC 只在 **leader** 上跑(`leadingFresh` 门)。判定一律以 `pd-ctl` 集群视图为准,不要拿 follower 的 `storage.gc.safepoint` 指标下结论(见 `finding-63` 的 leader-only 指标误诊教训)。共置时注意 `ip_local_port_range` 抢端口坑(`finding-multitenant-clustermesh`)。

---

## Test 1 — 服务名 + 有限 TTL(快,~1 个 GC 周期)

**起两个命名租户**(其余 flag 沿用 lab 的 quadlet:`--pd-addrs`、`--advertise-host`、端口、`--initial-cluster` 等):

```bash
kube-brain --keyspace=tenant-a --storage-gc-lifetime=5m  ...   # 激进
kube-brain --keyspace=tenant-b --storage-gc-lifetime=1h  ...   # 保守
```

等 ≤10min(leader 每 `min(lifetime,10m)` 续租一次),`pd-ctl service-gc-safepoint`:

**判定(PASS)**:
- [ ] 出现两条独立记录:`kubebrain-ks-tenant-a`、`kubebrain-ks-tenant-b`(名字正是 `resolveGCService` 的产物);
- [ ] 两条的 `expired_at` 都是**有限值 ≈ now + 30min**(= 3×10min TTL),**不是** `math.MaxInt64`;
- [ ] 两条 `safe_point` 不同:A 的更靠后(now-5m),B 的更靠前(now-1h)。

## Test 2 — 跨租户 min/clamp 不互删(核心,~15min 稳态)

沿用 Test 1 的 A(5m)/B(1h)。

**判定(PASS)**:
- [ ] 顶层 `gc_safe_point` == **min == B 的 safe_point(now-1h)**,而**不是** A 的(now-5m)。
      → 证明激进租户 A 无法把集群回收线推过保守租户 B 的需要,B 的 MVCC 不被 A 误删。

**(可选)前后对比,坐实 bug 真实存在**:同一 lab 换 **`origin/main` 二进制**(修复前,共用 `gc_worker`)跑同样的 A/B:
- 预期 `service_gc_safe_point` 里**只有一条 `gc_worker`**(A、B 互相覆盖,last-writer-wins);
- 顶层 `gc_safe_point` 会**跳到 last-writer 的点**(可能是 A 的 now-5m)→ B 的 5m~1h 历史被回收。
- 对比修复二进制的"两条独立记录 + 线钉在 B",差异一目了然。

## Test 3 — 离场租户 TTL 自愈(慢,~30–40min 等待)

在 Test 2 稳态(线钉在 B)基础上,**下线租户 B**(停掉 B 的所有 KubeBrain 副本,使其停止续租 `kubebrain-ks-tenant-b`)。

**判定(PASS)**:
- [ ] `pd-ctl service-gc-safepoint` 里 `kubebrain-ks-tenant-b` 记录在**最后一次续租后 ~30min 消失**(到 `expired_at`);
- [ ] B 消失后,顶层 `gc_safe_point` **不再被 B 顶住**,随 A 的续租**向前推进到 A 的点(now-5m)**。
      → 证明死租户不会永久冻住全 cell 的回收(这正是命名记录用有限 TTL 而非无限 TTL 的原因)。

> 想快速看趋势而不等满 30min:Test 1 已用 `expired_at` 字段**即时**证明 TTL 是有限的;Test 3 的完整过期只是把"有限"落到"真的过期了"。时间紧可只做前者。

## Test 4 — 默认 keyspace 回归护栏(快)

单独起一个 `--keyspace=""`(留空)的租户。

**判定(PASS)**:
- [ ] 它注册在 **`gc_worker`** 下,`expired_at` == **`math.MaxInt64`**(无限);
- [ ] 与修复前 `origin/main` 的单租户行为逐字节一致。
      → 证明默认/单租户路径零行为变化(即 30M 测过的那条)。

## (可选)Test 5 — 真实版本回收深度

Test 1–4 验的是 safepoint 机制(deterministic、快)。若要眼见 MVCC 真被/不被回收:
- 在 B(1h)里反复覆盖同一批 key 造 MVCC 版本;
- A 激进 GC 期间,确认 B 的旧版本**仍可被历史读到**(未被删)——因为集群线钉在 B;
- 再触发 tikv compaction(或等 compaction-filter),确认低于集群线的版本才被回收。
- 观测:`tikv-ctl --host <tikv> mvcc -k <key>` 看版本数;RocksDB 空间用 `finding-reverse-delete-gc` 的 du 陷阱注意事项(别被 space_placeholder 误导)。

---

## 通过标准汇总

| Test | 断言 | 快/慢 | 必做 |
|---|---|---|---|
| 1 | 两条 `kubebrain-ks-*` 独立记录 + 有限 `expired_at` | ~10min | ✅ |
| 2 | 集群 `gc_safe_point` == min == 保守租户 B | ~15min | ✅ |
| 3 | 离场 B 的记录 ~30min 过期,线随 A 前推 | ~40min | ✅(时间紧可降级为 Test1 的即时 TTL 检查) |
| 4 | 默认 keyspace 仍 `gc_worker`/无限 TTL | ~10min | ✅ |
| 2-AB | main 二进制复现"只一条 gc_worker + 线跳到 last-writer" | ~15min | 可选(坐实 bug) |
| 5 | 真实版本回收随集群线 | ~30min+ | 可选 |

**四条必做全 PASS → cherry-pick `19cb507c`+`07acb87e` 进 main。** 任一 FAIL → 记录 `pd-ctl` 输出回 dbaas 修。

## 常见坑

- **年轻集群 `now - lifetime` 触发 `physical<=0` 跳过**:`lifetime=1h` 在刚起的 TiKV 上仍是正的 TSO(PD 物理时钟是墙钟 ms),没问题;但别把 lifetime 配得比集群年龄还大到 TSO 变负,GC 会静默跳过(返回 0)。
- **时钟**:safepoint 的物理时间来自 **PD TSO**,不是本机时钟;多机对比看 PD 即可,无需担心节点间 NTP。
- **只看 leader**:非 leader 不推 safepoint,别在 follower 上找 `storage.gc.safepoint` 指标。
- **端口共置**:多租户 KubeBrain 共置一台机时收紧 `ip_local_port_range`,防出站抢 cilium/其它端口。
