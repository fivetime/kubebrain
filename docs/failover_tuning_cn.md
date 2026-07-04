# Failover 无主窗口调优（KubeBrain leader 切换）

## 现象与根因

在满载 + KubeBrain leader 失败切换的组合下，实测（104 核 / 3 PD + 3 TiKV / 3 节点 k3s）观察到的天花板：

- **KubeBrain 本身无恙**：不泄漏、不重启，新 leader 在 ~15–30s 选出，数据完整。
- **但 k3s 控制面进程会退出**（`leaderelection lost`）：KubeBrain 的无主窗口叠加满载下已升高的写延迟（TiKV 单 region 热点，~1–2s/批），超过了 k3s controller-manager 的租约续期窗口。`docker start k3s-server` 后集群从 KubeBrain 持久数据完整恢复。**过载的 etcd 也会一样**，这是数据存储层 failover 的固有边界，非 KubeBrain 缺陷。

无主窗口 = **(死 leader 的租约过期)** + **(继任者获取并就绪)**。其中最大的一项是 `LeaseDuration`：被 `--force` 杀掉的 leader 来不及释放租约，继任者必须等满 `LeaseDuration` 才能获取。

## 两个调优杠杆

### 杠杆 1（KubeBrain 侧，产品代码）：缩短选举时长

新增可配置 flag（默认保持历史值 8/5/1s，不设即行为不变）：

| flag | 默认 | 含义 |
|---|---|---|
| `--leader-lease-duration` | `8s` | 死 leader 租约被继任者接管前的持有时长（**主导无主窗口**） |
| `--leader-renew-deadline` | `5s` | 续期截止；**同时是 #39 写栅栏的自我栅栏边界** |
| `--leader-retry-period` | `1s` | 续期/获取的重试周期 |

启动时强制校验 `RetryPeriod < RenewDeadline < LeaseDuration`（保证自我栅栏严格早于租约过期，防脑裂）。

**权衡（重要）**：调小 `LeaseDuration` → 切换更快，**但误切换更多**——leader 一次 GC 停顿 / TiKV 抖动只要超过 `LeaseDuration` 就会被误判掉线换主。偏偏在造成天花板的高负载下（写延迟已 1–2s），过小的 `LeaseDuration` 会制造更多误切换，**适得其反**。因此：

- 不要盲目调小默认值。仅在后端延迟稳定、可预期的部署里，可谨慎降到例如 `lease=4s renew=2s retry=500ms`。
- `RenewDeadline` 必须留足写延迟余量：一个健康 leader 必须能在 `RenewDeadline` 内成功续期，否则会自我栅栏、停写。满载写延迟 1–2s 时，`RenewDeadline=2s` 已偏紧。

### 杠杆 2（消费者 / 部署侧，零 KubeBrain 风险）：调大 k8s 控制器的租约容忍

针对"k3s 控制面崩"这个**具体症状**，最稳的止血是让 k8s 控制器**扛过**这段数据存储无主窗口，而不是逼 KubeBrain 更快切换。给 k3s server 传：

```bash
k3s server \
  --kube-controller-manager-arg=leader-elect-lease-duration=30s \
  --kube-controller-manager-arg=leader-elect-renew-deadline=20s \
  --kube-controller-manager-arg=leader-elect-retry-period=4s \
  --kube-scheduler-arg=leader-elect-lease-duration=30s \
  --kube-scheduler-arg=leader-elect-renew-deadline=20s \
  --kube-scheduler-arg=leader-elect-retry-period=4s \
  ...
```

（原生 kube-apiserver + 独立 controller-manager/scheduler 部署时，把对应 `--leader-elect-*` 直接加到各组件。）这样即使 KubeBrain 无主 15–30s，控制器也不丢自己的锁、进程不退出。对 KubeBrain 正确性零影响。

同理，节点 lease（kubelet `--node-status-update-frequency` / kube-controller-manager `--node-monitor-grace-period`）默认已有较大容忍（40s+），一般无需动。

## 推荐组合

1. **默认**：KubeBrain 保持 8/5/1s（稳，误切换少）。
2. **止住控制面崩**：优先用**杠杆 2**——调大 k3s/k8s 控制器 `--leader-elect-lease-duration`（如 30s）到明显大于 KubeBrain 的最坏无主窗口。零风险。
3. **确需更快切换**（后端延迟稳定）：再谨慎用**杠杆 1**小幅降 KubeBrain `LeaseDuration`，并压测确认满载下不诱发误切换。

## 实测结果（2026-07-04，104 核 / 3 PD+3 TiKV / 3 节点 k3s）

两个杠杆都做了真集群实测（`hack/etcd-client-compat/failoverwrite_test.go` 写探针 + kill 真 leader）：

- **杠杆 2 有效**：给 k3s controller-manager/scheduler 加 `--leader-elect-lease-duration=30s` 后，满载中杀 KubeBrain leader → **k3s-server 全程不退出**(`Up`、uptime 持续增长)、controller-manager `leaderelection lost`=0、`failed to renew`=0、3 节点持续 Ready。对照基线(无容忍)：controller-manager 丢锁、k3s-server ~15s 内退出。
- **杠杆 1 有效**：写服务无主间隙 —— **8s 租约 ≈ 6.6s(恢复 +9.1s)**;**4s 租约(`--leader-lease-duration=4s --leader-renew-deadline=2s --leader-retry-period=500ms`)≈ 2.5s**。缩短租约明显缩短窗口。
- **杠杆 1 的误切换代价，本环境未触发**：4s/2s 配置下,即便满载把**批量写延迟压到 ~2s**(贴着 2s 续期截止),再加码到 24 路 churn,KubeBrain leader **零误切换、零重启**(合计 >3.5min 满载)。原因:leader 租约续期是对锁键的小写,不排在 configmap 热点 keyspace 后面,续期路径始终 <2s。**结论:该 tradeoff 真实存在于原理上,但因"续期延迟 ≠ 批量写延迟",实测比预期更稳**。仍建议:降默认前压测确认你的后端锁键续期延迟留足 `RenewDeadline` 余量。

## 参考

- 无主窗口天花板的实测复刻与恢复：`hack/dev/k3s-load-depth.sh`（头注释）。
- #39 写栅栏与自我栅栏时序：`pkg/server/service/leader/leader.go`（`EpochAndLeadingFresh` / `renewDeadline`）。
