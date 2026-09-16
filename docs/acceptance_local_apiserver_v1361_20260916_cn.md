# 本地盘后端：真实 Kubernetes v1.36.1 apiserver 接入

## 结论及范围

2026-09-16，真实独立 kube-apiserver v1.36.1 经 mTLS 接入
`kubebrain-dbaas-test/kubebrain-local`，普通对象 smoke 与连续 Watch 测试均退出 0，
两轮清理后各自 prefix 为空、后端 lease 集合为 0、endpoint health 正常。

这是回环绑定、独立临时 PKI、AlwaysAllow 的测试 apiserver，不替换宿主集群的
控制面，不启动 scheduler/controller-manager。Deployment 测试 replicas=0，不能
据此宣称 Pod 调度、控制器收敛、完整 Kubernetes/k3s、生产鉴权或大规模兼容性通过。
本轮没有重启 apiserver 或切换 KubeBrain leader，不代表故障下连续性或长期 soak。

## 身份与来源

- apiserver 来自 [官方 v1.36.1 linux/amd64 发布地址](https://dl.k8s.io/release/v1.36.1/bin/linux/amd64/kube-apiserver)，
  与同源官方 `.sha256` 文件核对相等；执行 `--version` 精确返回 `Kubernetes v1.36.1`。
  SHA-256：`9b4dba0a5b945f1fe0ce18f47535c5ff0c46ae384f9222047bce39fe91b6023e`。
  未使用本机版本输出为占位符的 `/usr/local/bin/kube-apiserver`。
- runner 源码为 `05f528af`；服务器仍是已审核 bb89c3f8 镜像
  `ghcr.io/fivetime/kubebrain@sha256:50b9938fe3e5ad379136957438abe3c2d4bf250dc6c5973c869e0ef1341d1537`，默认 2PC。
  **不覆盖后续本地 KeepAlive 修复 690ca2b7 的运行时验收。**
- KubeBrain StatefulSet UID `7d760f53-5bb5-4429-a2f8-651b89665616`、generation 2；
  PD/TiKV v8.5.3 各三副本、独立 TopoLVM 后端。两轮前后三个 StatefulSet 的
  UID、generation 和完整 spec 均一致，结束时均 3/3 Ready。
- Service UID 固定检查为 `46181ff3-0d95-4a19-9742-61e780dc5853`；后端证书主机名
  `kubebrain-local-client.kubebrain-dbaas-test.svc`，本机转发端口 18379。
  主机名映射只绑定在私有 mount namespace 内，不修改宿主 `/etc/hosts`。
- apiserver 使用回环端口 18449；临时 CA 校验启用，后端 mTLS 使用现有专用测试
  client 身份。没有把凭据复制进仓库。

## 普通 smoke

独占前缀 `/registry-kubebrain-apiserver-local-v1361-msq4zz78`，等待 `/readyz` 后运行：

- Namespace、ConfigMap create/update/get/delete；观察修改后的 Watch 事件。
- 标签筛选 5 项、复合标签筛选 2 项、字段筛选 `batch-3`。
- 分页 list 5 项，集合删除后 0 项。
- Secret 类型读取为 Opaque，Kubernetes `coordination.k8s.io/Lease` 创建并更新。
- 零副本 Deployment 创建、标签更新和读取。

内部 smoke 与外层 runner 均为退出码 0。证据目录
`/root/.local/state/kubebrain/local-apiserver-smoke.MSQ4zZ78/`，含版本与摘要、
execution.log、前后 prefix/lease、StatefulSet JSON、endpoint health 和终态文件。

## 连续 Watch

独占前缀 `/registry-kubebrain-apiserver-local-watch-v1361-adpi8tfb`：
20 个 ConfigMap，各连续更新 10 次；初次分页 list 数量 20；预更新等待 10 秒；
`ALLOW_WATCH_RESTARTS=0`，观测 200 个 MODIFIED，最终 list 数量 20。

除了原 runner 的事件总数检查，本轮持有 Watch 文件描述符直至 runner 终止，
在工作目录清理后保留完整原始流，并以 `audit-watch.jq` 独立核验：

- 220 个事件：20 ADDED、200 MODIFIED、0 ERROR。
- `(对象名, data.version)` 精确等于 `soak-1..20 × 1..10`，无缺失或重复组合。
- MODIFIED resourceVersion 严格递增，namespace 均为本轮独占 namespace。

原始流 SHA-256：`b55399ca96af15eec91489423358d01923741b4a15a360ab15644609d4149741`。
runner、smoke 和证据采集器均退出 0。证据目录
`/root/.local/state/kubebrain/local-apiserver-watch.aDPI8tFB/`，包括原始流、
审计 jq、apiserver 日志和与 smoke 相同的前后检查。启动阶段存在 post-start
hook 尚未完成的 readyz 日志，不把这次成功写成“全程没有任何未就绪日志”。
本轮完整性检查仅覆盖上述指定对象/版本，不是无限事件流或任意故障下的证明。

## 清理及后续

两轮临时 apiserver、Watch 和端口转发均已退出；18449/18379 无监听；两个 WORK_DIR
已删除，临时 PKI 随之删除、不可恢复。只清除本轮测试前缀，没有主动撤销未证明
所有权的 lease。旧 Ceph 实例未切换或删除，未变更任何 StorageClass 或数据卷。
已下载的官方二进制保留在 smoke 证据目录供后续复用，重用前须再次核对摘要；
两个 execution.claim 已消费，实验脚本不得直接重跑。

后续仍需真实 apiserver 重启、KubeBrain 故障/升级期间 Watch、独立 PD/TiKV 故障、
完整控制面及规模/长期测试，并对尚未发布的新源码单独做 CI 和运行时验收。
