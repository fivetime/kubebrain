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
