# KubeBrain 固定基线控制面调度测试

2026-09-17，首次将[参考控制面负载](acceptance_reference_controlplane_20260917_cn.md)
接到本地盘 KubeBrain。**操作阶段通过，但租约清理失败，整体退出 70；不是完整验收通过。**

## 版本和实际范围

工具源码 `346217b5`，被测实例仍是恢复后的 `bb89c3f8`：
`ghcr.io/fivetime/kubebrain@sha256:50b9938fe3e5ad379136957438abe3c2d4bf250dc6c5973c869e0ef1341d1537`。
没有切换镜像，不把结果算作当前源码或 `63e0bd48` 候选的验收。
`kubebrain-local` UID `7d760f53-5bb5-4429-a2f8-651b89665616`，generation 16、Ready3；
后端仍为独立本地盘 PD/TiKV，默认 2PC。通过固定 Pod 的本机转发和完整 mTLS 接入，
集群 ID `7686251028133611667`、成员 ID `3358157933` 以十进制字符串核验。
隔离 hosts 文件只绑定到私有 mount namespace，没有修改宿主机 DNS 或 hosts。

控制面三个组件均为已核验 v1.36.1，apiserver 使用 Node/RBAC 并禁用匿名认证。
两组件实际身份匹配，均不能创建 Deployment；controller-manager 使用控制器
ServiceAccount 凭据。Deployment → ReplicaSet → 三 Pod 的 UID 所有权链、
observedGeneration、PodScheduled 以及审计中的实际创建者/绑定者全部通过。
仅模拟一个 Ready 节点，无 kubelet 或 KWOK；不证明容器 Running、Deployment
Available、多控制器覆盖、HA、故障恢复或规模性能。

## 操作期间发现的错误

07:03:49 UTC controller-manager 两次记录：
`DataLoss: leader txn proxy returned invalid range payload ... count 0 for 1 key-values`。
控制器重试后完成，并非零错误。该错误与旧基线已知的
[Txn 失败 Range Count 缺陷](txn_failure_range_count_cn.md)一致；该修复在后续源码中，
本轮没有部署，不能据此宣称修复回归。ReplicaSet 日志还出现两次
`read version ... is not as new as written version ...`；仅记录观测，不凭一次重试
日志推断为数据丢失或认定所有 stale-read 场景已修复。

## 清理与残留

session 5735 终态 70。内部结果为
`{"operation_exit":0,"backend_cleanup_exit":1,"runner_exit":70}`。
三个本地控制面进程先停止并回收，再核验后端身份、删除本轮前缀中的 211 个键，
空前缀验证通过。60 秒内租约列表仍不为空，没有放宽窗口，没有 Revoke 或 KeepAlive。

07:05:52 UTC 独立只读检查（session 54894，退出 0）：

| Lease ID（十六进制） | granted TTL | 剩余 TTL | 附着键 |
| --- | --- | --- | --- |
| `0003a0ae033e7c02` | 3660s | 3535s | 无 |
| `0003a0ae033e7c03` | 3660s | 3540s | 无 |

这是长 TTL 空租约，不能仅因删完测试键就称所有资源清理完成，也不能把自然租约
保留本身判断为产品错误。预计约 08:05 UTC 到期仍须实时核验；该时间不是过期证明。
本轮保留默认事件 TTL，没有为了通过清理而调低 TTL。两条租约未消失前，不能满足
下一轮共享后端“零租约”准入；不要再次执行已消费的驱动。

外层后置核验通过：原 StatefulSet UID/generation/spec、所有命名空间 Pod 的
UID/containerStatuses、PV UID/spec/phase 前后一致。没有删除卷、改变旧 Ceph 数据
或修改后端配置。18383 转发及 18453 API 端口已释放。日志、PKI 和审计保留在私有
证据目录，不上传认证材料。

## 证据与下一步

根目录：`/root/.local/state/kubebrain/controlplane-kubebrain-baseline.yf3BDAhi/`。
运行证据：`controlplane-kubebrain.e2noCvhQ/`，含审计、对象链、前缀、删除记录及
`result.json`。外层 `postflight.txt=POSTFLIGHT_UNCHANGED`，只读租约快照为
`lease-0003a0ae033e7c02.json`、`lease-0003a0ae033e7c03.json`。

后续先只读确认两条租约自然到期，保留本轮失败结论；再以已审计的新候选进行
同负载对比，并独立处理默认长 TTL 与测试清理契约之间的差距。不要以三 Pod
调度成功替代全部 etcd 语义或生产就绪要求。

## 上游语义复核

本轮使用的已验证官方 apiserver v1.36.1 二进制 `--help` 明确显示
`--event-ttl` 默认 `1h0m0s`。本地 Kubernetes 源码
`5b0cba2ee0da06385b12a9ae20cd0671ea3f860d` 的
`staging/src/k8s.io/apiserver/pkg/storage/etcd3/lease_manager.go` 中，租约申请 TTL
加上 `min(5% × TTL, 60s)` 的复用余量；对 3600s 即得到 3660s。
该源码核对解释了观测值，但不是逐笔 Grant 请求的抓包证明，也不把本地源码
版本自动视作已下载 v1.36.1 二进制的完整构建来源。

参考 etcd `5cd9f4ee13801e18825d661e5005ae599460bc3a` 的
`server/storage/mvcc/kvstore_txn.go` 删除路径调用 `lessor.Detach`；
`server/lease/lessor.go` 的 Detach 仅移除 key 附着索引，不删除 leaseMap 条目。
所以“键已删除但租约仍存在”不能单独证明不兼容；提前撤销或将 DeleteRange
改成自动撤销空租约反而会改变可重新附着的租约语义。

清理器回归新增自然过期后成功、超过 60 秒后拒绝、LeaseList 失败三个分支，
仍禁止 Revoke/KeepAlive。测试只在专用测试 shell 中推进 SECONDS，并模拟租约
自然消失，不改变真实驱动窗口或集群 TTL。控制面组合 race 测试通过（7.947s），
子模块 vet 通过。这些测试不证明本轮两条实际长 TTL 租约已经过期。

产品侧新增 `TestDetachedEmptyLeaseExpiresNaturallyWithoutPublicRevision`，分别经
点删除和前缀删除解绑最后一个键，确认存活租约仍有持久记录；随后不执行 Revoke、
KeepAlive 或修改内部时间，等待真实定时器自然过期，核对 LeaseLeases 清除、
持久元数据移除、TimeToLive=-1 和公开 revision 不额外增加。race 连跑三次通过
（13.381s），产品包 vet 通过。首次编译因新增测试漏导入 fmt 失败，补齐后重跑；
不是产品行为的红绿复现。测试使用内存后端和短 TTL，不代替 TiKV 后端一小时
残留租约的真实到期验证。本轮未修改产品租约行为或集群参数。
