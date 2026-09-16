# 独立 apiserver 测试 PKI

`hack/dev/apiserver-smoke.sh` 与 `hack/dev/apiserver-watch-soak.sh` 新增
`APISERVER_PKI_MODE=ephemeral`，用于不依赖 kind 控制面的本机 apiserver 测试。
默认仍为 `kind`，现有调用方式不变。in-cluster runner 尚未适配本模式。

新模式要求显式指定可执行的 `APISERVER_BIN`，且在访问后端前验证该要求。
不会从 Docker 自动提取二进制或复制真实集群的 PKI。调用方仍须审核二进制的
版本、来源和摘要；“文件可执行”本身不是供应链或版本验证。

`create-apiserver-test-pki.sh` 仅接受不存在的绝对目录，使用 OpenSSL 生成独立
客户端 CA、front-proxy CA、各用途 leaf 证书和 service-account 签名密钥。
证书有效期一天；服务端 SAN 仅含 `127.0.0.1` 与 `localhost`，客户端证书仅有
clientAuth，用于原 runner 的回环绑定、AlwaysAllow 临时 apiserver，不是生产
鉴权配置。目录 0700、私钥无 group/other 权限，已有目录拒绝覆盖。

新模式下 kubeconfig 使用 `certificate-authority`，ready 探测使用 `curl --cacert`，
不跳过服务端证书校验。旧 kind 模式保持原来的 TLS 行为，不能把这项改进描述为
消除了所有 runner 的 skip-verify。

PKI 位于本轮独占 WORK_DIR 内，随原 cleanup 删除。不要单独在共享目录生成，
不要将 CA 私钥、客户端私钥或 kubeconfig 提交到仓库。

## 调用参数

在既有唯一 prefix、显式 mutation opt-in、独占 WORK_DIR/端口等参数之外设置：

```sh
APISERVER_PKI_MODE=ephemeral
APISERVER_BIN=/absolute/path/to/audited/kube-apiserver
ENDPOINT=https://backend-name-matching-certificate:3379
ETCD_CAFILE=/private/path/backend-ca.crt
ETCD_CERTFILE=/private/path/backend-client.crt
ETCD_KEYFILE=/private/path/backend-client.key
```

新生成的 apiserver PKI 不替代后端 mTLS 凭据。通过端口转发连接后端时，仍需保证
ENDPOINT 主机名与后端证书 SAN 匹配，不可用跳过验证规避。现有 lease 清理遵循
[只读收敛规则](apiserver_cleanup_ownership_cn.md)，不撤销无法证明所有权的 lease。

## 证据与待办

本地测试验证证书链、server/client EKU、证书/私钥配对、两套 CA 隔离、回环 SAN、
错误 IP 拒绝、目录/私钥权限、已有 PKI 拒绝覆盖，以及两个 runner 缺少显式二进制
时在后端访问前失败。测试不启动真实 apiserver，不替代对象生命周期或切换验收。

`hack/etcd-client-compat` 模块运行
`go test -race . -run 'Test.*(Runner|APIServer|KubernetesVersionMatrix)' -count=1`
通过，9.611 秒；三个变更 shell 文件通过 `bash -n`，补丁通过 `git diff --check`。

本机 `/usr/local/bin/kube-apiserver` 的只读检查输出
`Kubernetes v0.0.0-master+$Format:%H$`，SHA-256 为
`a86e3b1a2392adb4f1752625ce233e032e0c9e3667261e5ecf6b2a5323e8b8ae`。
没有据此推断其 Kubernetes 发布版本，也没有启动它进行验收。
后续需取得并核验目标版本二进制，准备精确后端连接与证据目录，再执行本地盘
独立 TiKV/PD 的真实 apiserver smoke/watch/重启/切换验收。
