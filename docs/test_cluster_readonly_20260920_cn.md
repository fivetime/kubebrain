# 2026-09-20 专用测试集群只读检查

本次仅通过已有专用 kubeconfig/context 查询资源，另检查本机调试证书
有效期；未部署镜像、修改协议、申请实验认领、初始化磁盘或注入故障。
原始 API 结果保存在私有目录
`/root/.local/state/kubebrain/readonly-cluster-20260920.CiyULV8T`，
采集时间见该目录 `observed-at`。资源是顺序读取，并非原子快照。

观察范围为命名空间 `kubebrain-dbaas-test` 的本地存储实例：

- `kubebrain-local` StatefulSet UID 为
  `7d760f53-5bb5-4429-a2f8-651b89665616`，3/3 Ready。
- 模板镜像仍为
  `ghcr.io/fivetime/kubebrain@sha256:50b9938fe3e5ad379136957438abe3c2d4bf250dc6c5973c869e0ef1341d1537`；
  未发现包含 `1pc` 或 `async` 的容器参数。此项不是事务协议的运行时证明。
- 三个 `kubebrain-local-*`、三个 `kb-local-pd-*` 和三个
  `kb-local-tikv-*` Pod 的容器均 Ready；KubeBrain 和 TiKV 的重启计数为 0，
  PD-0 计数为 1，其余 PD 为 0。该计数不是本次实验造成的重启结论。
- StorageClass `kubebrain-local-lvm` 对应的 12 个 PVC 均为 Bound。
  旧 Ceph 实例仍保留，本次未迁移、删除或使用它进行验收。
- 查询 `kubebrain-fault-owner` ConfigMap（`--ignore-not-found`）返回空；
  该命名空间的 CiliumNetworkPolicy 列表为空，上述九个 Pod 无
  `kubebrain.io/fault-owner` 标签。这不证明其他网络策略或 Cilium 数据面
  没有影响，也不代替恢复验证或新的实验认领。

本机 `topolvm-preparation.7q92U9MR/local-tls` 的 client/peer 证书到期为
2026-09-23 21:45:05 UTC，probe/info 到期为 21:45:06 UTC；CA 到期为
2026-10-16 21:45:05 UTC。这里只检查本机证书，不声称其与所有运行中 Pod
的挂载内容一致。后续实验前须重新核验有效期、挂载身份和 TLS 连接；本次
未轮换证书，未读取或写入私钥内容到文档。

Ready/Bound 只能证明此次资源状态，不能证明 etcd 协议兼容、磁盘延迟、
候选镜像准入或原 30 秒故障验收通过。完整故障执行 CLI 的在线准入、
其他证据留存、Join 和恢复后显式释放仍待实现与验证。

## 04:09 UTC 进程身份复核

2026-09-20T04:09:54Z 通过同一专用 kubeconfig/context 重新 GET Pod 列表，
将九个本地存储实例 Pod 分别与 03:30 UTC 原始快照配对，实际执行仓库
`hack/production/same-pod-process.jq`。三个 KubeBrain、三个 PD 和三个
TiKV 的九次比较均返回 true：UID、完整 spec、Pod IP、容器 ID/imageID、
重启次数及 running.startedAt 与先前快照一致。容器均 Ready，PD-0 的
累计重启计数仍为 1，其余为 0；没有发现两次快照间进程身份变化。

完整当前 Pod 列表、九份比较输入、九份结果、采集时间及 SHA-256 清单位于
`/root/.local/state/kubebrain/readonly-process-recheck.Jt9TeLWq`。本次只有
一次 Pod 列表读取，没有部署、协议或存储修改、故障注入或恢复操作。
本次没有重新查询 PVC，因此不将此前 Bound 状态当成本次新观测结果。
两次快照一致不是连续可用性监控，也不证明候选镜像、TLS、term、故障
隔离或原 30 秒门限通过；集群仍是既有固定基线，不是尚在 CI 的候选版本。
