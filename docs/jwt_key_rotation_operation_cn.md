# JWTKeyRotation 持久 Operation 设计

## 目标与边界

`JWTKeyRotation` 把 `validate-jwt-key-rotation.sh` 的 phase A/B/C 直接门禁接入现有
`KubeBrainOperation` claim、lease heartbeat、attempt fencing 和 terminal CAS。它负责一个实例内 immutable
双 key Secret 与 StatefulSet 的受审发布、固定 JWT 的持久保存、TTL 等待和最终 receipt 提交；不负责外部 KMS
生成、跨实例调度或审计对象锁归档。

不得复用 `CertificateRotation` 类型。JWT phase B 后存在按秒计算且必须持续 heartbeat 的等待窗口，接管者还必须持有
原 phase A 旧 token，故其恢复状态和权限合同不同于证书 begin/overlap/complete。

## 不变量

1. 请求者只能在 `kubebrain-operations` 创建一个 immutable 参数 Secret 和一个未批准 Pending Operation；不能读取或修改
   数据面 Secret/StatefulSet。
2. Operation 名固定为 `jwt-key-rotate-<request binding SHA-256 前 20 hex>`，type 为 `JWTKeyRotation`，
   `requestedBy=platform:jwt-key-rotation`，`maxAttempts=5`，并要求专用 requester/approver/executor 身份。
3. old/new signing material 在请求前已置于 executor 的规范、非 symlink、受访问控制 workspace；参数只记录规范路径和
   SHA-256。executor 在任何发布前做 1 MiB 上限、源权限、双摘要冻结。
4. 数据面 Secret 从 phase A 前开始同时包含 old/new 两个非空字段，`immutable=true`；phase C 只移除 StatefulSet
   `verify-key` 参数，不删除 Secret 中 old key。旧 key 的销毁是最终 receipt 归档后的独立 KMS 操作。
5. 三阶段均由同一个 operation ID、实例、StatefulSet UID、Secret UID/resourceVersion/data digest、endpoint 数组、TTL、
   clock skew 和期望副本数约束。任何漂移只能 retry/fail closed，不能自动改写历史 receipt。
6. phase A 使用 old signer/new verifier，签发并持久保存 old JWT；phase B 使用 new signer/old verifier，必须复用同一 old
   JWT 并持久保存一个 new JWT；phase C 等到 phase B receipt 的 `earliest_retirement_at_unix` 后才发布 new-only，随后
   持久保存 fresh new JWT。
7. token 文件固定为 0600 普通文件并使用 no-clobber 原子发布：
   `<state-dir>/<operation-id>.old.jwt`、`.phase-b-new.jwt`、`.phase-c-new.jwt`。已有文件只允许在线复验和摘要复用，禁止
   重新 Authenticate 后覆盖，否则接管者无法证明“同一旧 token 已撤权”。
8. 每个外部发布、rollout 等待、token 签发、gate、TTL 等待和 receipt 冻结都由 heartbeat 子循环覆盖。停止 heartbeat
   后只允许 final heartbeat 紧邻 `succeed --receipt-sha256`；最终续租失败时不得提交 succeed/retry。

## 参数 Secret schema v1

参数 JSON 必须是严格顶层字段集合，最大 64 KiB：

```json
{
  "state_dir": "/var/lib/kubebrain-operation/jwt-key-rotate-<id>.state",
  "receipt_output": "/var/lib/kubebrain-operation/jwt-key-rotate-<id>.operation.receipt.json",
  "kubebrain_namespace": "instance-a",
  "kubebrain_statefulset": "kubebrain",
  "key_secret": "kubebrain-jwt-rotation",
  "old_key_field": "old-key",
  "new_key_field": "new-key",
  "old_key_source": "/var/lib/kubebrain-operation/keys/old",
  "new_key_source": "/var/lib/kubebrain-operation/keys/new",
  "old_key_sha256": "<64 hex>",
  "new_key_sha256": "<64 hex>",
  "endpoints": ["https://member-0:2379", "https://member-1:2379", "https://member-2:2379"],
  "expected_replicas": 3,
  "jwt_ttl_seconds": 300,
  "max_clock_skew_seconds": 2,
  "probe_range_key": "/kubebrain/jwt-rotation/probe",
  "probe_cacert": "/var/lib/kubebrain-operation/tls/ca.crt",
  "probe_cert": "/var/lib/kubebrain-operation/tls/client.crt",
  "probe_key": "/var/lib/kubebrain-operation/tls/client.key",
  "probe_server_name": "kubebrain-peer.instance-a.svc",
  "probe_cacert_sha256": "<64 hex>",
  "probe_cert_sha256": "<64 hex>",
  "probe_key_sha256": "<64 hex>",
  "data_kube_context": "",
  "data_kubeconfig_path": ""
}
```

TLS 三文件可同时为空以支持显式 plaintext 开发环境；只要任一 TLS 字段非空，CA 必填且 cert/key 必须成对。生产 admission
profile 必须要求 HTTPS endpoint 与非空 TLS 字段。endpoint 数必须等于 `expected_replicas`，均唯一且只允许单 endpoint
字符串；TTL 为规范正 int32，skew 为规范非负 int32，二者与当前时间相加不得溢出 int64。

Operation 参数不包含 root 密码、JWT 或 KMS 解封凭据。token 签发器使用独立 projected credential/短期 broker，输出只允许写入
上述固定 workspace 文件；stdout/stderr、Operation message 和 receipt 都不得包含 token/key 明文。

## executor 状态机

```text
claim + freeze parameters/keys/TLS
  |
  +-- final receipt exists --> verify phase A/B/final + online phase C --> terminal CAS
  |
  +-- phase B exists -------> heartbeat wait until earliest retirement
  |                            -> reconcile phase C -> persist fresh token -> phase C gate
  |
  +-- phase A exists -------> reconcile phase B -> persist/reuse B token -> phase B gate
  |                            -> heartbeat wait -> phase C
  |
  `-- no receipt -----------> reconcile phase A -> persist/reuse old token -> phase A gate
                               -> phase B -> wait -> phase C
```

发布器必须读取当前 StatefulSet 和 Secret 的 UID/resourceVersion，使用 resourceVersion/JSON Patch precondition 更新，等待
observedGeneration、replicas、readyReplicas、updatedReplicas 和 current/update revision 收敛。它只能修改受管
`--auth-token` 参数、operation phase annotation、固定 Secret 引用和必要的 checksum annotation；镜像、其他 args/env、
ServiceAccount、security context、volume 或 selector 漂移时拒绝。每次发布结果写一个独立严格 JSON publish receipt，gate
receipt 保持 A5492 的既有 schema。runner 最后原子发布独立
`kubebrain.jwt-key-rotation.operation.receipt.v1`，绑定三张 publish receipt、phase A/B/final gate receipt、参数 SHA、Operation
UID/attempt 和各自摘要；terminal CAS 提交该 composite receipt 的 SHA-256，不能仅凭 rollout status stdout 或修改既有 gate
schema。

phase A 发布失败可安全重试到 A。phase B 混合窗口只能继续收敛到 B，或由显式 rollback policy 把全部副本收敛回 A；不得留下
部分 signer。phase C 发布失败应重新引入 old verifier 并收敛到 B，随后由新 attempt 重新确认时间与在线证据；不能删除
immutable Secret 或重新使用已经终态的 operation ID。

## 权限与 admission

- requester：仅 `kubebrainoperations create/get` 与 operation namespace 内参数 Secret `create/get`。
- executor queue：现有 operation worker Role，加 `JWTKeyRotation` type→专用 ServiceAccount 的 status admission 映射。
- parameter broker：`kubebrain-jwt-key-rotation-executor` 只能获取 claim 中 type=JWTKeyRotation 的参数。
- 数据面 publisher：目标 namespace 中精确 StatefulSet/Secret/Pod 的 get/list/watch，以及 Secret create 和 StatefulSet patch；
  admission 要求 operation ID annotation、固定对象名、immutable 双 key Secret 和允许字段差分。禁止 delete Secret/StatefulSet、
  exec、pods/delete、任意 namespace 或通配资源。
- executor Deployment：replicas 初始为 0、非 root、read-only rootfs、seccomp、drop ALL、持久 workspace、受限 projected
  parameter/token issuer credential，不挂载 Kubernetes admin kubeconfig。

## 实现与验收清单

- CRD enum、queue `requiresApproval/isSupportedOperationType`、parameter broker、worker/audit admission 和 operation audit type
  allowlist 同步加入 `JWTKeyRotation`。
- 新增专用 requester RBAC/admission、request 脚本及确定性 immutable 参数 Secret 测试。
- 新增 publisher、runner、executor ServiceAccount/Role/Deployment；runner 测试覆盖 claim identity、参数精确 schema/大小、路径逃逸、
  key/token 漂移、阶段恢复、TTL heartbeat、publish/gate 失败、最终 receipt mutation 和 terminal fencing。
- admission/RBAC 测试证明 requester 无数据面权限、executor 无 delete/exec/跨 namespace 权限，且非专用身份不能提交、批准、读取参数
  或更新状态。
- 真实三副本演练必须故障注入：phase B 发布一个 Pod 后 executor 重启、TTL 等待中 claim 接管、phase C publish 后 terminal CAS
  前 fencing。最终仍只能有一条 receipt 链、一次终态，旧 JWT 全拒绝、新 JWT 全接受，Pod 零非预期重启。
