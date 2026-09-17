# 参考 etcd：真实 apiserver 连续更新期间 leader 故障（2026-09-17）

## 结果与限制

本轮参考 etcd **操作、Watch 完整性、清理及归档均通过**，外层驱动退出 0。
同一官方 kube-apiserver v1.36.1、20 个对象各 100 次连续更新，在 leader 进程被
SIGKILL 后仍完成 2000 个 MODIFIED，无公共 Watch 重启、无失败 PATCH 重试。

这与 [KubeBrain 实验](acceptance_apiserver_continuous_fault_20260917_cn.md)在 `soak-9`
PATCH 返回 leader changed、仅收集 8 个 MODIFIED 的结果不同。因此目前不能将
KubeBrain 失败解释为“参考 etcd 在这种负载下必然如此”。**可用性差异仍未解决。**

这是单次观察，不证明参考 etcd 在所有故障时刻都不会返回未决写错误。两次实验不是
严格的性能 A/B：参考后端为本机三进程 loopback HTTP、没有 TiKV/PD 或远端 TLS/
port-forward；KubeBrain 为远端三 Pod。本次 SIGKILL 的参考 leader 没有重建，剩余
两成员继续保持 quorum；KubeBrain 的 leader Pod 由 StatefulSet 自动重建。
故障发生时的 MODIFIED 数也不同（参考 7，KubeBrain 3）。不得由此断言特定代码根因，
也不改变原验收门限或用无条件重放事务掩盖失败。

## 固定来源与执行范围

- 参考 `/root/etcd` 源码 `5cd9f4ee13801e18825d661e5005ae599460bc3a`，实际二进制
  版本 `3.8.0-alpha.0`、Go `1.26.5`；通过仓库 provenance 校验。这不是 etcd 3.6 发布版验收。
- kube-apiserver v1.36.1 SHA-256
  `9b4dba0a5b945f1fe0ce18f47535c5ff0c46ae384f9222047bce39fe91b6023e`。
- 执行时 KubeBrain 仓库 HEAD `cefd1e38`；使用现有 `hack/dev/apiserver-watch-soak.sh`，
  原生证据归档、ephemeral PKI，未在运行时修改脚本。
- 新建私有数据目录，client 端口 13479/13481/13483，peer 端口 13480/13482/13484，
  apiserver 18449；全部 loopback。启动前检查端口空闲，只向自己创建且未回收的子进程发信号。
- 单 follower 入口 `ref2`（13483），初始 leader `ref1`；term 2。
- `OBJECTS=20`、`UPDATES=100`、`PRE_UPDATE_SLEEP_SECONDS=0`、`ALLOW_WATCH_RESTARTS=0`。
  使用唯一 registry prefix 和 namespace，未接入控制器或 scheduler。
- 01:24:34 UTC，在 Watch barrier 之后、更新循环未完成且观察到 7 个 MODIFIED 时
  SIGKILL `ref1`。后检 leader 已改变、term 3、存活入口健康。

## 最终证据和清理

原生 `result.json`：`operation_exit=0, cleanup_failed=0, archive_failed=0, runner_exit=0`。
独立重放 `verify-apiserver-watch.jq` 再次确认 20 × 100、2000 MODIFIED、integrity passed。
清理后 prefix 为空、LeaseList 为 0；未撤销不明归属 lease。所有本轮 etcd 子进程已等待
结束，七个监听端口为空；包含 PKI/kubeconfig 的临时 work 目录已移除。
新建的本地 etcd 数据目录及原始日志保留，不与共享实例混用。

私有证据 `/root/.local/state/kubebrain/reference-apiserver-fault.XwWNe8VK/`：
`run.sh`、`provenance.log`、`etcd-version.txt`、`topology.txt`、`fault-time.txt`、
`modified-at-fault.txt`、`pre-fault-status.json`、`post-fault-status.json`、
`archive/`、`watch-audit.json`、`post-prefix.json`、`post-leases.txt`、`runner.exit`。
会话 58874 已终态 0，claim 已消费，不可重跑。

Watch SHA-256：`42df0e607808a38959b5b56e4a1097051d80b0188febb37038ec4a59d0e3ec63`。
apiserver 日志 SHA-256：`2db480903926e47ce8c2ba4e85664c64ec9a22163e177934857849e880f2e166`。

## 下一步

KubeBrain 下一轮必须补齐故障前后所有前端成员的日志与身份连续性证据，定位失败 Txn
是否已转发、是否因共享 peer 连接关闭而取消、以及新 leader 的可服务时间。
本轮 KubeBrain 原始证据不足以确认这些阶段，不能追溯假造已消失 Pod 的日志。
保持已接纳事务不盲目重放的边界，再根据可重现证据修复额外的可用性缺口。
