# 隔离完整控制面验收准备

最新终态：[ba1dab70 候选报告](acceptance_controlplane_ba1dab70_20260917_cn.md)。
驱动 83888 退出 70：操作通过、固定 60 秒租约清理失败；原镜像恢复及预拉取
任务清理均成功，独立后置核验通过，本轮 12 个可证明临时卷已回收。两条空长
租约仍待自然到期，所有测试进程及转发已停止。下文“正在运行”均为历史记录。

08:05 UTC：旧基线自然过期观察已成功（两条 TTL=-1、零租约、空前缀），
候选已重新读取原始快照并通过只读准入 `deploy-verify.u2WMZyPf`。
部署驱动 session 83888 正在执行 `deploy-execute.rxHhBfrd`，已进入隔离预拉取
阶段；这是正在运行的尝试，不是候选 Ready 或测试成功证明。后续必须跟踪同一
驱动至终态，核验恢复、清理及后置状态。以下 HOLD／等待描述保留为准备历史。

2026-09-17 07:53 UTC：旧基线的只读自然过期观察仍在运行（session 97069，
`controlplane-kubebrain-baseline.yf3BDAhi/expiry-observer.43ahMqUt`），第 15 次
采样两条空租约 TTL 为 704/709 秒；尚不能宣称零租约。候选新增一次性
`admit-and-execute.sh`，要求该观察终态成功、端口释放后才读取新鲜原始快照，
完成只读准入再释放 HOLD；退出时恢复 HOLD 并记录终态。当前观察未完成时的
拒绝分支已验证（退出 1，未创建准入 claim 或调用部署）。工作负载封装还会在
恢复镜像前采集三个候选成员的本轮日志，并检查采集前后 Pod 身份一致；采集
失败不能报告整体验收成功。以上仅为私有操作脚本准备，未部署候选。

下一阶段 KWOK 的本机只读准备：PATH 中未发现 `kwok`/`kwokctl`；本地
`/root/kwok` 源码为 `099ce5faf29193ac19f0d7529103327c48570f20`，工作树干净，
go.mod 使用 Kubernetes 库 v0.36.1。这不是已构建或已核验的运行二进制。
源码 `pkg/kwok/cmd/root.go` 支持显式 kubeconfig、单节点或标签选择器管理、
节点租约及静态配置；未指定有效 kubeconfig 时存在后续回退路径，因此外层必须
先验证配置，不能依赖命令默认值隔离管理集群。后续应使用隔离 API 专属身份及
受限 RBAC、显式节点选择器，核验真实控制器创建对象与 KWOK 状态更新的不同
审计写入者。仓库现有快速 Stage 的节点状态心跳约十分钟，不应不经验证就当作
默认 NodeLifecycle 控制器的健康心跳；需单独证明节点 Lease 持续更新及 Ready。
不得用 `derive` 直接注入 ReplicaSet/Pod 充当真实控制器规模验收，或将 KWOK 的
模拟 Running 宣称为真实容器运行。尚未启动 KWOK，也未改变本轮候选负载。

2026-09-17 后续：`ba1dab70` 两项 CI 已全部成功，独立镜像审计也已通过
（session 87512 退出 0，`release-ba1dab70.YKfEnhFq/audit.jLnRBmhQ`）。
发布 index 为 `sha256:d08ad362bef61f13ba00b15700ef8810854cfd3055b988e1bb6d52b7448f0fff`，
amd64 为 `sha256:e285f99dbd1885eb4287f4d06a8b8579f789302dadcc9b4505d6cbc183a5a599`，
arm64 为 `sha256:7280f22ae86774d316225097dbe88547c6f184cfd7a9b45dc6c3f89b4fbd3210`。
已验证清单摘要、实际 amd64 二进制源码/版本/构建信息、非 root 用户及同源 CI；
临时审计容器与提取二进制已清理。此结论只覆盖发布身份，不是部署验收。
候选仍保持 HOLD，等待旧基线租约自然过期及正式新鲜准入；以下 CI 运行中描述
为此前准备记录，不代表当前 CI 状态。

候选 `ba1dab70` 的同源 CI 为镜像 `35193444673`、探针 `35193444669`，仍在运行，
未部署。私有准备目录 `controlplane-candidate-ba1dab70.EaxkqpNF` 保持 HOLD；
准入、工作负载、恢复、后置检查及精确临时卷回收脚本已准备，尚无本轮候选发布
审计结论、正式准入或可回收卷清单。历史 Released 卷不进入本轮回收候选集。
实际执行前仍须重新获取原 spec/runtime/backend/PV/PVC 快照，并核验零租约；
这些准备不能作为未来持续有效的变更授权或完成证明。

最新实际结果：[bb89c3f8 基线共享后端测试](acceptance_controlplane_bb89c3f8_20260917_cn.md)
完成真实控制器和调度操作，但租约清理失败，整体退出 70，另有已知 Count 错误重试。
下文“尚未执行”等内容为准备历史，不代表此轮执行尚未发生；新候选尚未验证。

后续进展：[参考 etcd 控制面调度验证](acceptance_reference_controlplane_20260917_cn.md)
已通过真实 RBAC 身份、控制器派生对象及 scheduler 绑定的核验。下文保留准备历史；
KubeBrain 后端接入、KWOK/容器运行及完整控制面验收仍未完成。

2026-09-17：现有测试证明了独立 apiserver 的部分行为，尚未证明真实 controller-manager、
scheduler 联动及 KWOK 规模场景。不能用 apiserver-only 或直接写入派生对象替代真实控制器验收。

本机 `/usr/local/bin/kube-controller-manager` 和 `kube-scheduler` 均报告
`Kubernetes v0.0.0-master+$Format:%H$`，不作为官方 v1.36.1 验收二进制，未覆盖或删除。
依据 [Kubernetes 官方下载说明](https://kubernetes.io/releases/download/)，在隔离目录
下载固定 `v1.36.1/linux/amd64` 组件，匹配已验证的官方 apiserver v1.36.1，而非追随 latest。

| 组件 | SHA-256 |
| --- | --- |
| kube-controller-manager | `41a41cd08f8035661848e883479816d5e65f832a461c004840eb2cd54998af08` |
| kube-scheduler | `37729fdcd47c791cbebe413951150f58d36e05cbc759ad77e5cf66196fccbec8` |

下载地址为 `https://dl.k8s.io/release/v1.36.1/bin/linux/amd64/<组件>`，先获取对应 `.sha256`，
校验实际二进制后才执行 `--version`；两者均返回 `Kubernetes v1.36.1`。
保留 Go buildinfo、下载地址及逐组件收据；未验证 Sigstore 签名，不将 checksum 校验写成签名认证。
进程 session 50526 终态 0，私有目录 `controlplane-v1361.Ojgfumui`。仅执行版本检查，未启动服务。

后续需为隔离 apiserver 配套独立 PKI、kubeconfig、监听端口及存储前缀，并验证 RBAC、
真实 Deployment→ReplicaSet→Pod 调谐及调度。不要访问管理测试集群的 API 执行业务对象测试，
不要占用既有 6443 服务，不直接运行会部署另一套存储的 `hack/scale-lab/setup.sh all`。
当前 PATH 未发现 KWOK；不能因此宣称节点或 Pod Running 模拟已具备。
所有新组件运行和清理均需明确的进程及对象归属。当前持续负载回滚实验结束并清理前，
不向同一后端并发启动另一个控制面验收负载。

## RBAC 身份准备（2026-09-17 后续）

上述回滚实验现已结束、恢复并清理，见[回滚报告](acceptance_local_rollback_63e0bd48_20260917_cn.md)。
现有 apiserver smoke 使用 `AlwaysAllow`，不作为完整控制面 RBAC 验收。
`hack/dev/create-apiserver-test-pki.sh /absolute/new/pki --controlplane` 新增显式可选模式，
仅在新建的独立测试 CA 下生成以下客户端身份，不读取管理集群的 CA 或凭据：

| 文件名前缀 | CN | Organization |
| --- | --- | --- |
| admin | kubebrain-test-admin | system:masters |
| controller-manager | system:kube-controller-manager | 无 |
| scheduler | system:kube-scheduler | 无 |

组件身份依据本地 Kubernetes 源码 `5b0cba2ee0da06385b12a9ae20cd0671ea3f860d`
的 `plugin/pkg/auth/authorizer/rbac/bootstrappolicy/policy.go` 中默认用户绑定核对；
仍须在固定 v1.36.1 实际 apiserver 上验证 RBAC，不能以源码核对代替运行证明。
未来控制器需使用自己的 kubeconfig，并验证控制器服务账号凭据路径；不能为方便
把 admin kubeconfig 交给所有组件，也不应把控制器身份加入 system:masters。

默认单参数调用不新增这些凭据；未知模式、多余参数及已存在目录继续拒绝。
证书仅一天有效、clientAuth 用途、独立密钥与序列号，无 server SAN，私钥仅属主可访问。
新增测试在旧脚本上因未知模式退出 2；修复后在 `hack/etcd-client-compat` 子模块执行
`go test -race . -run '^TestEphemeral' -count=1` 通过（2.530s），`go vet .` 和 shell
语法检查通过。最初在根模块运行此测试因嵌套模块边界失败，不属于产品回归结果。
当前常规 CI 的 compat 作业仅编译这些测试，不宣称此结果来自 CI。

本次仅修改生成器并在临时目录验证，未为真实控制面签发持久凭据或启动服务。
仍待实现隔离进程启动/停止、RBAC 运行核验、真实 Deployment→ReplicaSet→Pod→调度、
证据归档及精确清理；不得用直接注入 ReplicaSet/Pod 或伪造 Running 替代控制器联动。

## 共享后端驱动已准备，尚未执行 KubeBrain 接入

`hack/scale-lab/controlplane-smoke.sh` 现在承载共同的 apiserver/RBAC/真实控制器/调度
工作负载；`controlplane-reference-smoke.sh` 保留为强制 reference 的兼容入口，
即使环境要求 kubebrain 也拒绝切换。默认仍使用独立本地参考 etcd。

共享模式需同时显式设置 `ALLOW_LOCAL_CONTROLPLANE_TEST=true`、
`CONTROLPLANE_BACKEND=kubebrain`、`ALLOW_MUTATING_CONTROLPLANE_BACKEND=true`。
另外必须提供 `CONTROLPLANE_ENDPOINT`（单一 HTTPS 地址）、`CONTROLPLANE_CA`、
`CONTROLPLANE_CERT`、`CONTROLPLANE_KEY`、`CONTROLPLANE_ETCDCTL` 绝对路径及
`CONTROLPLANE_ETCDCTL_SHA256`，以及预先核验的十进制字符串
`CONTROLPLANE_CLUSTER_ID`、`CONTROLPLANE_MEMBER_ID`。这些不能从任意首次响应
自动接受，外层操作所有者仍须核验 Kubernetes 实例、Pod、镜像和 TLS 身份。
组件二进制摘要、固定版本、独立 API 端口和私有工作目录要求与参考模式相同。

共享后端 helper `controlplane-backend.sh` 使用完整 TLS 校验；通过已核对参考
etcdctl 源码的 fields 输出比较 ID 字符串，避免大整数经 jq 舍入后误匹配。
正式创建 apiserver 前，检查随机本轮 `/registry-kubebrain-controlplane-…/` 前缀
为空且租约列表为零，保存本轮前缀收据。这个收据不是跨机器锁，仍要求专用后端
同一时间只有一个测试所有者；不得并行运行其他租约/故障验收。

退出时先停止并回收三个控制面进程，再核对同一后端身份，仅删除本轮带尾部斜杠
的前缀并确认空。等待租约自然过期最多 60 秒，不 Revoke、不 KeepAlive；身份漂移、
删除或租约检查失败均使清理失败，整体退出 70，`result.json` 分别记录操作与清理结果。
短调度测试不保证所有长 TTL 租约都会及时过期，因此不能只看操作通过消息。

本地共享后端十场景 race 测试通过（4.934s）：包括大于 2^53 的相邻集群 ID 拒绝、
重复 status、非空前缀、无 header、命令失败、已有租约、清理身份漂移、删除失败和
删除后残留。组合控制面测试通过（5.041s），独立授权拒绝另行验证；`go vet .` 通过。
模拟 CLI 测试不证明真实 TLS 或租约过期行为。

重构后参考运行 session 7005 终态 0，证据 `controlplane-reference.N0RaLL27`，
`result.json` 的 operation/backend_cleanup/runner 均为 0，测试端口已释放。
该次 backend_cleanup=0 表示 reference 模式无需共享后端清理，不能作为 KubeBrain
清理验证。**目前尚未使用这个新模式对共享 KubeBrain 后端执行实际控制面负载。**
