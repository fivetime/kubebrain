# 隔离完整控制面验收准备

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
