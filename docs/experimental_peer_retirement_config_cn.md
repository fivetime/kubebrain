# 实验性 peer 交接启动配置

默认关闭。显式指定 `--experimental-peer-retirement-config=/path/policy.json`
后，启动参数层向 Endpoint 传入 PeerRetirementOptions。此入口不表示已经
通过真实 TiKV 故障、原 30 秒门限或生产就绪验收；当前测试集群未启用。

文件最大 64 KiB，必须是普通文件（允许 Secret 投影的符号链接）。只接受
一个 UTF-8 JSON 对象，以下八个字段全部必填、大小写精确；拒绝未知字段、
重复字段（包括转义同名键）、尾随 JSON、null 及过深嵌套。错误不回显文件
内容或底层解析错误。普通文件的底层 I/O 仍可能阻塞，不承诺硬实时读取。

示意结构如下，占位符必须替换，不能原样部署：

```json
{
  "scope": "retirement-v1:<实际存储命名空间的 SHA-256>",
  "holder_pins": {
    "member1:3380": ["<member1 的 64 位十六进制 SPKI SHA-256>"],
    "member2:3380": ["<member2 的 64 位十六进制 SPKI SHA-256>"]
  },
  "endpoint_holders": {
    "https://member2:3380": "member2:3380"
  },
  "read_budget": "1s",
  "operation_budget": "1s",
  "send_budget": "250ms",
  "concurrency": 2,
  "requests_per_second": 4
}
```

- holder 必须与实际选举 identity 完全一致。CLI 使用 advertise-host 与
  peer-port 构造 `host:port`，不是 HTTPS URL。生产式测试应显式指定
  `--advertise-host`，不要依赖自动选择本机网卡地址。
- 本机 holder 必须有 pin。endpoint_holders 只列远端，禁止指向本机 holder，
  必须使用精确 HTTPS base URL，不带路径、用户信息、查询或片段。不同成员
  的配置文件因此不同，不可共享一个包含本机条目的全量远端表。
  转发凭据映射同时接受该配置 URL 及其精确 `host:port` 拼写，以兼容 CLI
  选举记录；后者仍强制 TLS，不接受显式 `http://`、未知地址或 DNS 别名推断。
- 2–17 个 holder，每个 1–8 个 pin，1–16 个远端。pin 是证书公钥 SPKI 的
  SHA-256，而非证书整体摘要、CN 或 IP。重复 pin、跨 holder 共用 key 均拒绝；
  轮换时可提前给同一 holder 配置不同 key 的多个 pin。
- 三个预算均为 Go duration 字符串，范围 `(0, 1m]`；concurrency 为 1–64，
  requests_per_second 为 `(0, 1000]`。没有隐式默认预算或错误容忍回退。
- scope 来自实际 backend resource lock 的 RetirementScope，绑定真实存储
  cluster ID、keyspace 和选举 prefix，算法见
  [retired_release_scope.go](../pkg/backend/election/retired_release_scope.go)。
  文件解析不代替后端绑定校验；启动构造器仍拒绝 scope 不匹配，且不会启动
  campaign。不能自行使用集群名称、namespace 或上述占位符代替实际 scope。

必须同时配置 `--peer-cert-file`、`--peer-key-file`、`--peer-trusted-ca-file`、
`--peer-client-cert-auth=true`，禁止 `--peer-allow-insecure=true`。出站可以
另指定 peer-client-cert/key-file；否则沿用 peer 证书。标准 CA、SAN、有效期
校验仍执行，pin 只是额外身份约束，不替代标准 TLS。证书、私钥、kubeconfig
不放入本文件或仓库。共享 peer 私钥的现有部署不能直接开启此功能。

策略在 Validate 和 Run 开始时读取，Run 的读取发生在创建存储客户端之前。
每次先清空旧策略；失败即返回错误，不保留先前成功读取的配置。正常运行后
scope、pin、远端映射和预算固定，修改文件不会在线重载这些策略，须重启。
peer TLS 材料的握手时重载是独立机制，不能误当作 pin 策略也会热更新。

传输使用既有实验性 peer 专用请求头/协议分类期限。此参数不调整公共端点、
选举租约、客户端重试或验收门限，也不会自动部署、迁移后端或变更磁盘。

## 离线计算 scope

先独立核验真实 PD/TiKV cluster ID、启动 keyspace 和传给 backend 的有效
选举 prefix，再运行：

```sh
go run ./hack/production/cmd/peer-retirement-scope \
  --storage-cluster-id=42 \
  --keyspace= \
  --election-prefix=/endpoint-pair
```

这是编码示例，不是测试集群参数。工具直接复用 resource lock 的版本化
编码函数，不连接网络、不读取存储或 Secret、不写文件；JSON 输出包含
scope 和输入，cluster ID 用十进制字符串保存，避免 uint64 被 JSON 消费者
转为浮点数时损失精度。三个参数均须显式传入，包括空 keyspace；不接受
零 cluster ID、空 prefix、溢出 ID 或位置参数。输出失败时命令返回失败。

`inputs_verified:false` 是刻意的：离线工具不能证明操作者输入来自目标
集群。特别注意 CLI 在非空 keyspace 时将默认内部 prefix 追加
`/ks-<keyspace>` 后才传给 backend；不要把未追加的 prefix、用户键前缀、
etcd 兼容层的合成 cluster ID 或 Kubernetes namespace UID 当作输入。
上线时 server 构造器仍会与实际 resource lock scope 比较，不匹配则拒绝。

## 专用测试集群的离线 PKI 准备

`hack/production/cmd/peer-retirement-test-pki` 是一次性测试材料生成器，
不是生产 CA、Kubernetes controller 或自动部署脚本。七个参数必须全部指定：

```sh
go run ./hack/production/cmd/peer-retirement-test-pki \
  --output-dir=/private/new-peer-bundle \
  --peer-service=peer.test.svc \
  --members=member-0,member-1 \
  --storage-cluster-id=42 --keyspace=tenant --election-prefix=/test \
  --peer-port=3380
```

以上为示例参数，不能直接用于现有集群。输出目录须不存在，父目录由操作者
可信控制；拒绝覆盖既有目录或 symlink。所有目录 0700、文件 0600。根目录
保存测试 CA cert/key；每个成员目录只含其 tls.crt、tls.key、ca.crt、policy.json。
成员 key 分别随机生成，SAN 包含该成员的 `<member>.<peer-service>` 和公共
peer service 名称，支持 serverAuth/clientAuth，策略 pin 对应真实叶证书
SPKI。CA 有效期 72 小时，叶证书 24 小时；部署前须重新检查剩余时间。

策略使用 scope 共用编码函数，包含全体公共 pin，但远端映射排除本机。
候选预算固定为 read/operation/send 各 1 秒、并发 2、每秒 4 请求；这些不是
性能验收结论或自动推荐的生产配置。生成失败不清理部分证据，也不重用旧
目录；最后写 COMPLETE 标记，表示本次流程完成，不表示断电持久化证明。

私钥不得进入 Git、CI 日志或普通测试文档，CA 私钥不得挂载进工作负载。
每个 Pod 只应获得自己的成员目录，不能直接挂载完整 bundle，使一个成员
获得其他 holder 的身份。若使用 subPath/subPathExpr 隔离目录，不能同时
声称 Secret 更新会自动投影到该挂载；必须明确重启策略或另行设计热轮换。
从旧共享 CA/证书迁移还需准备信任过渡、精确回退和独立验收，工具不处理
这些流程，也不创建 Secret/ConfigMap 或修改 StatefulSet。
