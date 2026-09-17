# 隔离完整控制面验收准备

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
