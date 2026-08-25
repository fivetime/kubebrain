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
3. requester 从规范绝对路径读取 old/new signing material，把材料与参数 JSON 一并写入同一个 immutable Secret；参数只记录
   固定 sibling field 名与 SHA-256，不记录 requester/executor 文件路径。executor 没有 Secret `get` 权限，只能以 projected
   ServiceAccount token 经 HTTPS parameter broker、并在 type/owner/attempt fencing 通过后逐字段取得材料；每份材料在使用前
   写入 0600 临时文件并复核大小和摘要。
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
  "request_id": "change-2026-jwt-1",
  "state_dir": "/var/lib/kubebrain-operation/jwt-key-rotate-<id>.state",
  "receipt_output": "/var/lib/kubebrain-operation/jwt-key-rotate-<id>.operation.receipt.json",
  "kubebrain_namespace": "instance-a",
  "kubebrain_statefulset": "kubebrain",
  "key_secret": "jwt-key-rotate-<request-binding-20-hex>-keys",
  "old_key_field": "old-key",
  "new_key_field": "new-key",
  "old_key_material_key": "jwt-old-key",
  "new_key_material_key": "jwt-new-key",
  "old_key_version_id": "kms/prod/jwt/versions/41",
  "new_key_version_id": "kms/prod/jwt/versions/42",
  "kms_receipt_public_key_sha256": "<64 hex>",
  "old_key_export_receipt_sha256": "<64 hex>",
  "new_key_export_receipt_sha256": "<64 hex>",
  "old_key_sha256": "<64 hex>",
  "new_key_sha256": "<64 hex>",
  "key_volume": "jwt-keys",
  "key_mount_dir": "/etc/kubebrain-jwt",
  "sign_method": "HS256",
  "endpoints": ["https://member-0:2379", "https://member-1:2379", "https://member-2:2379"],
  "expected_replicas": 3,
  "jwt_ttl_seconds": 300,
  "max_clock_skew_seconds": 2,
  "probe_range_key": "/kubebrain/jwt-rotation/probe",
  "probe_cacert_material_key": "probe-ca",
  "probe_cert_material_key": "probe-cert",
  "probe_key_material_key": "probe-key",
  "probe_server_name": "kubebrain-peer.instance-a.svc",
  "probe_cacert_sha256": "<64 hex>",
  "probe_cert_sha256": "<64 hex>",
  "probe_key_sha256": "<64 hex>",
  "data_kube_context": "",
  "data_kubeconfig_path": ""
}
```

TLS 三个 material key 可同时为空以支持显式 plaintext 开发环境；只要任一 TLS 字段非空，CA 必填且 cert/key 必须成对。参数
Secret 的 `.data` 只允许精确三种形状：`parameters.json+jwt-old-key+jwt-new-key`、再加 `probe-ca`，或再加完整
`probe-ca+probe-cert+probe-key`；所有值非空。单份输入最大 512 KiB，材料合计最大 700 KiB，以留出 Kubernetes Secret 编码和
metadata 余量。生产 admission
profile 必须要求 HTTPS endpoint 与非空 TLS 字段。endpoint 数必须等于 `expected_replicas`，均唯一且只允许单 endpoint
字符串；TTL 为规范正 int32，skew 为规范非负 int32，二者与当前时间相加不得溢出 int64。

old/new version ID 必须不同、各为 1..512 字符，首字符为字母或数字，其余只允许字母数字及 `._:/@+=-`，以兼容常见
KMS resource/version URI 且禁止空白与控制字符。version ID 与材料 SHA 都进入 deterministic operation binding；ID 是外部
KMS provenance 的不可变声明，不单独证明该版本当前 enabled/primary/revoked 状态，状态真实性必须由后续 provider receipt 证明。

requester 还必须提供两张 provider export receipt 和 pinned Ed25519 public key。envelope 精确为
`kubebrain.jwt-kms-export-envelope.v1`，其 base64 payload 必须是无未知字段、无 trailing value 的规范
`kubebrain.jwt-kms-export.v1` JSON，签名覆盖 payload 原始字节。payload 绑定 request ID、instance、version ID、材料 SHA-256、
`state=enabled`、export/expiry Unix 时间；有效期最多 15 分钟，允许最多 60 秒未来时钟偏差。验证器使用 `O_NOFOLLOW` 打开有界普通
文件，trust key/receipt 禁止 group/world 写，材料禁止 group/other 任意访问。requester 在验证前把材料、receipt、trust key 与 TLS
输入冻结为 0600 私有副本，验证、摘要和 Secret 创建只使用同一冻结字节；验证失败不得调用 Kubernetes。

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
`kubebrain.jwt-key-rotation.operation.receipt.v3`，显式绑定外部 request ID、old/new KMS version ID、三张 publish receipt、phase A/B/final gate receipt、
参数 SHA、Operation UID/attempt 和各自摘要；terminal CAS 提交该 composite receipt 的 SHA-256，不能仅凭 rollout status stdout 或修改既有 gate
schema。

phase A 发布失败可安全重试到 A。phase B 混合窗口只能继续收敛到 B，或由显式 rollback policy 把全部副本收敛回 A；不得留下
部分 signer。phase C 发布失败应重新引入 old verifier 并收敛到 B，随后由新 attempt 重新确认时间与在线证据；不能删除
immutable Secret 或重新使用已经终态的 operation ID。

## 权限与 admission

- requester：仅 `kubebrainoperations create/get` 与 operation namespace 内参数 Secret `create/get`。
- executor queue：现有 operation worker Role，加 `JWTKeyRotation` type→专用 ServiceAccount 的 status admission 映射。
- parameter broker：`kubebrain-jwt-key-rotation-executor` 只能获取 claim 中 type=JWTKeyRotation 的参数，以及固定 allowlist 中的
  `jwt-old-key`、`jwt-new-key`、`probe-ca`、`probe-cert`、`probe-key`；每次材料读取都重新校验专用 ServiceAccount、claim owner 与 attempt。
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

## A5493 实现进度

提交 `e6ba7c3d` 已交付第一项纵向原语 `kubebrain-jwt-rotation-publisher`，并纳入正式 Dockerfile build/runtime stage。
它只接受 phase A/B/C 的精确前驱状态；读取 0600、普通非 symlink、1..1 MiB 且 SHA-256 与审批参数一致的 old/new key；
phase A 创建带 operation annotation 的 immutable、精确双字段 Secret，后续阶段绑定其 UID、resourceVersion 和规范 data
SHA-256。发布前要求 StatefulSet 已完整 rollout，并校验精确副本数、唯一 `kubebrain` container、唯一 TTL/auth 参数、只读
Secret volume/mount、UID 和剔除三项受管 annotation/auth 参数后的 template baseline。

StatefulSet 更新使用 resourceVersion 与原 auth 参数双 JSON Patch precondition，只改 auth 参数及三项受管 annotation；正常相邻
阶段必须收敛到不同 rollout revision，崩溃接管可复用已收敛 revision。publish receipt 使用 0600 临时文件、file sync、
no-clobber hard-link 和 directory sync，绑定
old/new key 摘要、Secret/StatefulSet identity、template baseline、前后 revision、auth 参数摘要和前一 receipt 摘要。进程在
patch 后、receipt 前退出时可识别已收敛状态并生成 `reconciled_existing=true` 的恢复 receipt；已有 receipt 则必须重新核对在线
状态，不能覆盖。`kubectl` stdout/stderr 各自最多保留 2 MiB。

包级生命周期、接管恢复、race、vet 和构建检查通过；负向测试覆盖不安全 key/receipt 权限、源摘要漂移、Secret 漂移、重复 auth
参数、TTL/模板/annotation 漂移、未完成 rollout、StatefulSet UID 替换和并发 patch。提交前 inventory 为 615 项、四片
142/171/155/147；提交后四片 Go/墙钟秒分别为 132.132/138.196、374.984/381.067、224.626/230.688、
383.174/389.255，全部通过。

提交 `c8352bb5` 在 publisher 之上交付持久纵向链路：CRD enum、queue approval、operation audit allowlist、worker type admission、
parameter broker identity/NetworkPolicy、专用 requester RBAC/admission 与 deterministic request 脚本同步启用；executor
Deployment 保持 `replicas: 0`，采用专用 ServiceAccount、PVC、projected broker token 和只读 JWT 认证 Secret；不再挂载
可执行 token issuer hook。

runner 对 64 KiB 参数做精确 schema 与 SHA-256 双读冻结，在自身边界重新校验 workspace、1 MiB 上限、symlink、私钥权限、TLS
配对/摘要和 HTTPS member 集合。它持久复用 old/phase-B-new token，phase C 强制 fresh new token；所有 issuer、publisher、gate
和 TTL wait 均有 heartbeat 子循环及进程组 fencing。phase B receipt 在移除 old verifier 前由 runner 再次严格校验 receipt chain、
token 摘要与 `observed+TTL+skew` 算式。最终 composite receipt 以 file sync、no-clobber hard-link 和 directory sync 绑定 Operation
UID/生成 attempt、参数摘要、三张 publish receipt 和三张 gate receipt；崩溃接管可复验旧 attempt receipt 后用当前 fenced attempt
提交 terminal CAS，已有 composite 不得覆盖。

请求、生命周期、attempt 接管、heartbeat 杀进程组、CRD/manifest、broker type binding、approval/audit 与 requester inventory
测试通过；代码提交前 inventory 为 618 项、四片 143/171/155/149，提交后四片 Go/墙钟秒为
123.990/130.091、356.779/362.890、225.884/231.982、369.691/375.798，全部通过。

提交 `c76cd02d` 增加受限 `--rollback-phase-c-to-b` 原语。只有 phase C 尚无 publish receipt、phase B receipt 严格有效且
StatefulSet UID/template baseline、immutable Secret 和审批 key 摘要均未漂移时，才允许以 resourceVersion/auth 参数双
precondition 把 C 或部分 C 收敛回 B；已有 C receipt 一律拒绝回滚，重复调用已收敛的 B 幂等成功。runner 对首次 C 发布失败先
重做一次收敛确认，排除“rollout 已完成但响应丢失”，仍失败才在 heartbeat/fencing 下调用回滚，回滚成功后 retry 交给新 attempt
重新确认 TTL 与在线状态。包级 lifecycle、幂等、receipted-C 拒绝、race/vet 通过；618 项四片提交后 Go/墙钟秒为
131.751/137.834、358.316/364.426、224.576/230.654、371.212/377.320，全部通过。

提交 `cafc9bcf` 固定 key Secret 名为 `<operation-id>-keys`，requester 不再接受调用者指定 Secret，runner 再次校验该绑定。
集群级 fail-closed publisher admission 要求 dedicated instance namespace 与专用 executor identity；Secret CREATE 必须是 operation
绑定的 immutable、精确双字段对象；StatefulSet UPDATE 冻结 scale/selector/strategy/claims、Pod metadata、volume/init/sidecar、
identity/placement/network/security policy 以及 KubeBrain image/command/ports/env/mount/resources/probes/lifecycle，只允许唯一
`--auth-token` 参数和三项受管 annotation 变化。实例 Role renderer 只授予目标 StatefulSet get/patch、目标 Secret get，以及受上述
admission 约束的 Secret create，不授予 list/watch/update/delete/exec 或跨 namespace 权限。operation 完成并归档后，平台必须删除
该 operation-labeled Role/RoleBinding，避免专用 ServiceAccount 累积历史实例授权。

publisher admission 已纳入 requester guardrail 的 apply 顺序和 compiled-policy check，静态 manifest、模拟 apply/check、确定性 RBAC
渲染及未绑定 Secret 拒绝测试通过。代码提交前 inventory 为 620 项、四片 144/171/155/150；提交后四片 Go/墙钟秒为
134.441/140.577、377.166/383.298、235.153/241.317、395.412/401.558，全部通过。真实 API server 上的 CEL
type-check、server-side dry-run 和 `auth can-i` 不能用本地 YAML 解码替代。

提交 `d0910168` 用正式 `kubebrain-jwt-token-issuer` 替换可执行 hook。issuer 只从 dedicated Secret mount 下读取
`username`/`password`，支持 Kubernetes atomic-writer symlink 但要求最终目标仍位于 credential root、只读且 other 不可访问；密码
不进入 argv、环境变量或日志。它对一个审批 endpoint 调用官方 client/v3 `Authenticate`，错误文本不透传服务端/凭据内容，JWT
以 0600 临时文件、file sync、no-clobber hard-link、unlink 和 directory sync 发布；已有安全单链接 token 幂等复用，竞争写不能
覆盖。TLS 复用参数摘要冻结后的 CA/client cert/key。

对照 etcd `server/auth/jwt.go` 的 `assign`（以当前 private key 签发）以及 KubeBrain phase B 的 new signer + old verify-key 扩展，runner
时序固定为 `publish A → Authenticate old → gate A → publish B → Authenticate new → gate B → TTL → publish C → Authenticate fresh
new → gate C`，不再在 rollout 前用本地 key 离线造 token。Secret mount 固定 0440、read-only，镜像正式 build/copy issuer；单元测试
覆盖 symlink root 逃逸、可写 credential、错误脱敏、无覆盖发布和恢复复用，runner 黑盒测试锁定发布/签发顺序。代码提交前
inventory 为 620 项、四片 144/171/155/150；提交后四片 Go/墙钟秒为 138.141/144.247、374.569/380.698、
235.408/241.539、385.385/391.513，全部通过。

提交 `e674e764` 完成 publisher admission 的真实 Kubernetes API Server 验收。首次 server-side apply 直接发现 inline YAML message
中的未引用逗号被解析成伪字段；修复后 API Server 又拒绝 map `.filter(k,v,...)`，再改为双向 `.all`；合法 auth-only dry-run
随后暴露 absent optional field 的 `no such key`。最终 policy 对 StatefulSet/Pod/container 可选字段逐项使用 presence+value 等价，
以 CEL variables 绑定唯一 KubeBrain container，并增加 validation 对象只能含 `expression/message` 的 YAML 回归断言。

在 Kind Kubernetes v1.36.1 上，两项 policy 均达到 `observedGeneration == generation` 且 `typeChecking={}`，binding 均为 `Deny`。
专用 identity 的合法 immutable 双 key Secret CREATE 与唯一 auth 参数/三项 annotation StatefulSet PATCH 均通过 server dry-run；
mutable Secret、错误 operation-derived 名称、错误 identity、image drift 均被对应 policy 拒绝。实例 Role 的实测矩阵只允许目标
StatefulSet get/patch、目标 Secret get 和 namespace 内 Secret create；StatefulSet update/其他对象 patch、其他 Secret get、delete/list、
Pod get/exec 及跨 namespace patch 全部为 `no`。一次性 namespace、Role/Binding 和两组 policy/binding 已全部删除并确认无残留。
620 项提交后四片 Go/墙钟秒为 123.645/129.741、356.454/362.577、225.825/231.951、375.868/381.955，全部通过。

提交 `ac8e6ae0` 将上述 JWT policy 验收扩展到完整 Operation requester/worker 控制面。真实 Kind 干净队列暴露 foundation 先应用
parameter broker repair-queue Role/Binding、却未创建 `kubebrain-repair-operations` 的确定性失败；foundation 现先以统一
`app.kubernetes.io/part-of=kubebrain` label 创建主/repair 两个队列 namespace，再应用 policy、等待 compiled exact Deny，最后授予
RBAC 与零副本 workload。

requester guardrail 的 inventory/check 原本把 self-contained TiKV repair alert policy 计入 38 项，但 apply 不会安装它；同时 17 组
requester RBAC 会在 policy observed/type-check 前立即授予。修复后从同一 alert receiver manifest 的 client-dry-run JSON stream 中只
筛选两项 policy/binding，绝不创建其 2 副本 Deployment/Service/PDB/NetworkPolicy；待全部 38 项 policy
`observedGeneration==generation`、无 expression warning、binding 精确 `Deny` 后，才应用 17 组 requester RBAC 与 alert identity 的
SA/Role/RoleBinding 子集。

Kubernetes v1.36.1 上标准 foundation apply、guardrail apply/check 最终通过，输出为
`checked 38 compiled Deny policies and 18 requester RBAC/admission identities`；所有 Operation API/broker/archiver/executor 仍为
0 副本，alert receiver Deployment 未创建。旧集群首次迁移遇到 client-side→server-side field ownership conflict；因测试 CRD 无
Operation，live 验收显式 force-migrate 既有 foundation manager 后再运行标准脚本，生产升级不得无审计地照搬该动作。临时数据面
namespace 已删除。620 项提交后四片 Go/墙钟秒为 134.699/140.814、367.435/373.553、227.896/234.072、
378.059/384.167，全部通过。

提交 `71ed8081` 关闭 requester 本地路径无法跨 Pod 交付给 executor 的缺口。requester 现在把参数和 old/new key、可选 probe TLS
材料放入同一个 immutable Opaque Secret；参数仅绑定固定 material field 与 SHA-256，不再泄露或依赖主机路径。admission 只接受
精确 3/4/6 字段形状且拒绝空值、额外字段和不完整 mTLS。已有同名 Secret 的幂等路径比较完整 `.data`，任一材料漂移均失败。

parameter broker 新增 `/v1/material`：沿用 projected token TokenReview、executor ServiceAccount→operation type 映射及
owner/attempt claim fencing，并只为 JWTKeyRotation 开放五个固定字段；错误统一为不可用，不在响应中暴露 Secret 细节。
`operationctl material` 只接受 HTTPS origin、受限 key、受信 CA 与有界响应。runner 在 heartbeat/process-group fencing 内逐份下载，
以 0600 捕获并复核 1 MiB 上限和审批 SHA 后才交给 publisher/探针；executor 仍无 Kubernetes Secret `get` 权限。

Kind Kubernetes v1.36.1 上 requester policy 达到 generation/observedGeneration 1/1、2/2 且无 CEL type warning；合法三字段 Secret
server dry-run 放行，缺失 key、额外 key 和不完整 TLS 均拒绝。race、vet 与定向 production 测试通过；完整未分片包因累计运行到
Go 默认 10 分钟总超时停在既有 info certificate scrape 用例，该用例单独 1.965 秒通过。权威 620 项四分片提交后 Go/墙钟秒为
143.327/149.438、373.654/379.762、231.023/237.133、382.421/388.548，全部通过。

提交 `440f8f73` 增加 KMS version provenance 强绑定。requester 必须提供不同且规范的 old/new version ID，两个 ID 同时进入
operation 名 binding 与严格参数 schema。`operationctl material` 对 signing material 强制携带 version；broker 不只匹配请求字段，
还从摘要绑定的参数 JSON 独立验证两个 version 均规范、互异，并要求请求 version 精确匹配对应 old/new 字段。TLS material 禁止
伪带 KMS version。最终 composite receipt 升级为 v2 并显式记录两个 version ID，已有 v1/篡改 receipt 均不能幂等复用。
定向测试、race、vet 通过；620 项提交后四片 Go/墙钟秒为 146.864/152.978、373.425/379.459、236.001/242.057、
381.470/387.540，全部通过。该证据固定 provenance 声明，但尚未验证外部 KMS 对 version 状态的签名/在线证明。

提交 `e15b1d0a` 交付正式 `kubebrain-jwt-kms-export-verifier` 并纳入 runtime image。requester 现在强制验证 old/new 两张短期
Ed25519-signed export receipt；identity/state/time/signature/material 任一不匹配都在 Kubernetes 调用前失败。receipt、provider public
key 和材料 SHA 同时进入 deterministic operation identity 与参数摘要；runner 的精确 schema 要求三项 provenance digest 非空且
old/new receipt 不同。所有输入先冻结到私有临时目录，消除验证后再从可变源路径创建 Secret 的窗口。验证器单元/文件系统边界、
requester fail-before-Kubernetes、race、vet、build 和镜像合同通过；621 项提交后四片 Go/墙钟秒为
133.081/139.209、364.557/370.752、231.373/237.505、381.369/387.503，全部通过。该实现验证 provider 已签署的 enabled/export
事实；provider API 调用本身、primary promotion、旧版本 revoke 及其终态 receipt 仍未交付。

提交 `83f22d48` 增加正式 `kubebrain-jwt-kms-export-client` 与唯一推荐入口
`request-jwt-key-rotation-from-kms.sh`。client 只接受无 userinfo/path/query/fragment 的 HTTPS origin，以 0600 短期 Bearer token
和 pinned CA 调用固定 `POST /v1/jwt-key-versions:export`；请求 JSON 精确绑定 request/instance/version。它禁止 redirect，限制
20 秒总预算、15 秒 HTTP timeout、1 MiB response、512 KiB material 与 64 KiB receipt，拒绝未知/trailing JSON 和错误 media type，
不透传 provider body。material/receipt 以 0600、file sync、hard-link no-clobber 和 directory sync 成对发布，第二项失败会移除本次
新建的第一项。wrapper 在 WORK_DIR 私有临时目录顺序 export old/new，随后交给固定 requester 做签名验证和 Operation 提交，退出
必删明文捕获；caller 不能混入直接 key/receipt 输入。

首次 `83f22d48` 提交后四分片都被 requester inventory 正确拒绝：新增 `request-*.sh` 尚未登记，错误统一为
`requester executable inventory drifted`，没有被误判成产品测试回归。提交 `0d12301d` 将 wrapper 显式纳入权威 inventory，并移除
可由环境替换的 request command，只允许委托仓库固定 `request-jwt-key-rotation.sh`。wrapper 正向测试使用真实下游 requester，负向
证明任一 export 失败不会调用 Kubernetes。最终 623 项四片为 145/172/156/150，提交后 Go/墙钟秒为
137.219/143.410、361.246/367.394、235.131/241.288、380.005/386.233，全部通过。export 调用链已交付；provider promotion/revoke
仍必须由不向 executor 授予 KMS 写权限的独立生命周期身份实现。

提交 `74fc263a` 修复 promotion/revoke 前置审计缺口：此前 external request ID 只参与 deterministic operation hash，无法从 v2
receipt 直接恢复，生命周期控制器不能证明 KMS mutation 对应哪张变更单。requester 现把规范 request ID 加入严格参数；runner
重新验证其语法，composite receipt 升级为 v3 并显式绑定 request ID。已有 v2 receipt 不会被新 schema 幂等接受。623 项四片
提交后 Go/墙钟秒为 131.244/137.375、375.550/381.678、228.913/235.046、395.870/402.005，全部通过。该提交只提供后续
KMS lifecycle request/operation 双重绑定，不代表 promotion 或 revoke 已执行。

提交 `98c40685` 交付正式 `kubebrain-jwt-kms-lifecycle-verifier` 并纳入 runtime image。它先严格解析 canonical v3 Operation receipt，
验证 request/operation/instance/attempt、old/new version 和七个 SHA-256，再以整张 Operation receipt SHA 作为 lifecycle 根。
provider envelope 为 `kubebrain.jwt-kms-lifecycle-envelope.v1`，Ed25519 签名 payload 为
`kubebrain.jwt-kms-lifecycle.v1`。promotion 必须无 predecessor、状态为 `new-primary`；revoke 必须引用同一 trust key 验证通过的
promotion envelope SHA、状态为 `old-revoked`，且 observed time 严格晚于 promotion。两类 receipt 都要逐项匹配 request、Operation、
instance、old/new version 和 Operation receipt SHA，发生在 Operation completion 后，有效期不超过 15 分钟并在当前窗口内。
未知字段、trailing/noncanonical JSON、签名/状态/顺序/时间/摘要漂移和 symlink/权限越界全部 fail closed。623 项提交后四片
Go/墙钟秒为 131.943/138.139、354.991/361.134、226.506/232.659、374.056/380.174，全部通过。该提交固定可信终态证据合同；
实际 mutation client、专用凭据/RBAC 与归档流程仍未交付。

提交 `b3650e6f` 交付正式 `kubebrain-jwt-kms-lifecycle-client` 和
`finalize-jwt-kms-lifecycle.sh`。client 从 canonical v3 Operation receipt 派生全部 identity/version/SHA，不接受调用者重述这些字段；
以独立 0600 lifecycle Bearer token、pinned CA、禁 redirect 的 HTTPS client 调用固定
`POST /v1/jwt-key-versions:promote|revoke`。promotion 请求 predecessor 为空；revoke 请求携带 promotion envelope SHA。
请求/响应 exact JSON、media type、20 秒总预算、15 秒 HTTP timeout、64 KiB receipt 和不透传 provider body 均 fail closed；签名
envelope 以 0600、fsync、hard-link no-clobber 和 directory sync 发布。

finalizer 先冻结 Operation receipt，只在 promotion receipt 经正式 verifier 证明 `new-primary` 后才允许发 revoke；任一 promotion
验证失败都保证零 revoke 调用。已有 promotion/revoke receipt 会重新验证并幂等跳过对应 mutation，支持响应落盘后的进程接管；
不同内容不能覆盖。625 项四片提交后 Go/墙钟秒为
125.977/132.112、352.488/358.624、227.869/234.002、368.369/374.501，全部通过。当前仍缺真实 provider 实机演练、专用
lifecycle credential 的 Kubernetes/外部身份部署与轮换，以及将两张终态 receipt 写入 Object Lock 审计归档。

提交 `9c113dd8` 补齐 lifecycle 终态证据的代码级 Object Lock 归档入口。正式 verifier 在同一次 revoke 链验证成功后，以
exclusive create 生成 `kubebrain.jwt-kms-lifecycle-artifact.v1`：artifact 内嵌原始 Operation、promotion、revoke 三份回执，
同时绑定三份 SHA-256、request/operation/instance、old/new version 及 completion/promotion/revoke 时间。归档 wrapper 先冻结
全部输入和 trust key，再调用 verifier；任何验签或链校验失败都不会启动对象存储写入。

上传复用 immutable blob 合同：固定 identity 派生对象键、`If-None-Match:*`、SHA-256 metadata、Object Lock retention、唯一
version 和不可覆盖 receipt；随后用 `blob-read` 枚举并精确读取唯一版本，逐字节比较远端 artifact。627 项提交后四片
Go/墙钟秒为 131.387/137.465、359.730/365.805、230.512/236.605、376.890/382.983，全部通过。该提交证明代码路径，尚未证明
真实 provider mutation、真实 Object Lock bucket 写入/保留、独立 lifecycle credential 的部署与轮换；这些 live 门禁仍开放。

当前仍保持 disabled-by-default，不能直接规模化上线：本提交只实现受限 Kubernetes Secret→broker→executor 交接；外部 KMS
生成/export、version promotion/revoke 与认证 credential Secret 创建/轮换/撤权演练，三处真实
接管/fencing 故障注入也未完成。在这些门禁关闭前不得把 executor 扩容到非零，也不得对租户宣称自动轮换生产就绪。
