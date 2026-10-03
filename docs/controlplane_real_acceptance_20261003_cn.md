# 固定候选真实 Kubernetes 接入验收（2026-10-03 UTC）

本次现有控制面用例**通过**：候选与参考两侧功能退出 0；候选自然租约
清理退出 0；基线完整恢复及此次临时卷清理核验完成。只证明下述固定场景，
不代表六类验收全部通过或全面生产就绪。

## 场景、身份与结果

- 产品源码 `6c295888a8ef5ea0763eb8d0b1c0597b83211a3e`。
- 候选镜像 `ghcr.io/fivetime/kubebrain@sha256:76e25a1e6dea2fcd9e4bffdc01317d1c74ef43dcd0d6e797f40a257a3c48b1b8`。
- 三副本部署 revision `kubebrain-local-59b9d759d9`，既有 TopoLVM 本地独立盘
  TiKV/PD 后端；namespace `kubebrain-dbaas-test`。
- Kubernetes API Server、controller-manager、scheduler 固定 v1.36.1；使用
  留存的已核验哈希二进制，不使用本机报告 v0.0.0-master 的 `/usr/local/bin`。
- 对标 etcd 提交 `5cd9f4ee13801e18825d661e5005ae599460bc3a`。
- 入口：`hack/scale-lab/controlplane-reference-smoke.sh` 与
  `controlplane-smoke.sh`；`CONTROLPLANE_KWOK=true`、
  `CONTROLPLANE_POD_REPLACEMENT=true`。
- 真实控制器完成 Deployment/ReplicaSet 调谐和三个 Pod 的调度；删除一个
  精确 UID/resourceVersion 绑定的测试 Pod 后，原 Deployment/ReplicaSet 和
  两个幸存 Pod 保持，60 秒内产生并调度一个替代 Pod；状态及审计断言均通过。
- KWOK 仅模拟 Node/Pod Ready 状态，**没有真实容器执行或 HA 验收结论**。
- 候选连接保持完整 mTLS 验证、固定 cluster/member 身份和独占随机前缀。

## 自然清理与恢复

功能完成后停止本次控制面写入者，删除专属前缀；两条空租约自然到期，
未 KeepAlive、未 Revoke。初始最大 GrantedTTL=3660 秒，固定额外清理
预算 60 秒，未刷新截止时间。最终 LeaseList 为 0，`result.json` 的
`operation_exit`、`backend_cleanup_exit`、`runner_exit` 均为 0。

随后通过 UID/resourceVersion/完整 spec 条件保护恢复保存的原基线，核验
spec 完全一致、revision `kubebrain-local-568bd68448`、3 副本 Ready，未启用
1PC/async commit。临时本机 hosts 解析项已删除，18383/18453/13579/13580
无监听进程。本次 12 个退役 4Gi TopoLVM 临时卷，经归档 PVC/Pod UID、
所有权及无活跃引用核验后回收，最终 PV 列表确认全部不存在。临时卷数据
不可恢复，原始证据保留；没有操作产品数据卷或重新初始化磁盘。

## 可复现入口与证据

本机证据根目录 `/root/.local/state/kubebrain/controlplane-6c295888.ToNOk97g/`：

- `controlplane-reference.H8Vhjax5/`：参考侧功能、替换审计及最终退出结果。
- `controlplane-kubebrain.7qqNScy7/`：候选侧审计、替换验证、清理预算和最终结果。
- `run-candidate.sh`：固定二进制路径/哈希、证书路径、身份及入口命令。
- `deploy.sh`、`restore.sh` 与对应 JSON：部署和完整恢复证据。
- `reclaim-during-restore/`、`reclaim-final/`、`pvs-final.json`：精确临时卷回收证据。

重现时须从健康基线重新核验环境、使用新私有证据目录及专属随机前缀，
不能直接覆盖以上证据文件。证书、私钥和 kubeconfig 留在仓库外。
本报告不将参考侧或单个场景通过泛化成故障转移、升级/回滚或备份类别通过。
