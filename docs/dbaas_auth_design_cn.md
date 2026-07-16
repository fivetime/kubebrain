# KubeBrain DBaaS Auth 设计

## 目标与对标

目标是兼容 etcd v3.7 Auth API 和数据面权限语义，不只是让 `etcdctl user`
命令返回成功。源码基线：

- `/root/etcd/server/auth/store.go`：用户、角色、权限、auth revision；
- `/root/etcd/server/etcdserver/api/v3rpc/auth.go`：管理 RPC 与管理员检查；
- `/root/etcd/server/etcdserver/apply/auth.go`：KV/Txn 权限检查；
- `/root/etcd/server/etcdserver/api/v3rpc/watch.go`：Watch 建立和持续鉴权；
- `/root/etcd/api/v3rpc/rpctypes/error.go`：公开 gRPC 错误契约。

DBaaS 的 mTLS/IAM 只控制谁能连接实例，不能替代 etcd key-range RBAC。

## 安全不变量

1. Auth 开启前必须存在 `root` 用户和 `root` 角色绑定；否则返回 etcd 同款错误。
2. Auth 开启后，缺失、无效、过期或旧 auth revision token 不得访问数据。
3. 权限撤销返回成功后，任何副本都不得继续使用旧权限服务新请求。
4. Range/DeleteRange/Watch 权限按完整 `[key, range_end)` 检查，不能只检查首键。
5. Txn 必须检查所有 compare 与可能执行的 Then/Else 嵌套操作，避免分支探测越权。
6. Watch 在创建时和权限 revision 变化后都要重新校验；撤权后必须取消已有 watch。
7. Auth 元数据不消耗用户 MVCC revision、不进入用户 Range/Watch/Compact keyspace。
8. 密码只保存 bcrypt hash；日志、metrics 和错误不得暴露密码或 bearer token。
9. Auth 状态以 TiKV internal KV 为唯一真相，可在 KubeBrain 全部重启后恢复。
10. 任何缓存或刷新失败都必须 fail closed，不能退回未鉴权路径。

## 持久化布局

全部使用 backend keyspace 派生后的 internal KV：

```text
auth/config              -> {enabled, revision, tokenSigningKeyVersion}
auth/users/<encoded>     -> authpb.User
auth/roles/<encoded>     -> authpb.Role
auth/tokenkeys/<version> -> encrypted/signing key material
```

用户名和角色名使用长度前缀或 URL-safe base64 编码，禁止通过 `/` 构造跨记录键。
配置、用户和角色更新需要新的 internal compare-and-swap batch 原语：一次事务验证
旧 `auth/config.revision`，写入对象并把 auth revision 加一。不能用多个
`InternalPut` 顺序提交，否则故障时会出现对象已变但 revision 未变，旧 token 继续
有效。

## 多副本一致性

每个副本维护一个只读权限快照：`enabled/revision/users/roles`。快照不可独立轮询后
直接生效，因为权限撤销后的轮询窗口会继续放行。

采用以下顺序：

1. Auth mutation 只在 Leader 执行，通过 internal CAS batch 持久化新 revision。
2. 提交后把 auth revision 写入独立、可重放的 internal auth event log。
3. 所有副本按 revision 重放并原子替换权限快照；发现事件缺口或解析失败即把
   `authReady=false`，数据请求 fail closed，并从 internal KV 全量重建。
4. Leader 仅在本地快照达到提交 revision 后返回 mutation 成功。
5. follower 收到携带 token 的请求时，若 token revision 高于本地快照，先同步到
   至少该 revision；无法同步则返回 `Unavailable`，绝不按旧快照判断。

这样正常请求只做内存签名验证和区间权限判断，不给每次 KV 操作增加 TiKV 点读；
权限变更仍具有明确的一致性屏障。

## Token

第一阶段使用 HMAC-SHA256 bearer token，payload 至少包含：

```text
username, authRevision, issuedAt, expiresAt, keyVersion, nonce
```

签名密钥存 internal KV，KubeBrain Pod 只在内存持明文。AuthDisable、密码修改、用户
删除、角色/权限变更都会推进 auth revision；旧 revision token 因此整体失效。
后续可增加 JWT 公私钥模式，但不能改变权限 revision 的失效语义。

## 数据面接入

- Unary interceptor：解析 token、等待权限快照、把 AuthInfo 放 context；不直接
  判断请求 key。
- KV handlers：Range/Put/DeleteRange/Compact 分别检查读写或管理员权限。
- Txn：递归收集 compare、success、failure 中所有可能访问的 key range，并按操作
  类型检查；这比只检查最终选中分支更保守，且避免 compare 作为信息侧信道。
- Watch stream：每个 CreateRequest 检查 range；auth revision 推进时重新检查活动
  watch，不再允许的 watch 返回 PermissionDenied 并取消。
- Lease：Grant/KeepAlive/Revoke/List/TTL 属于已认证用户能力；带 lease Put 的 key
  权限仍由 KV Put 检查。是否允许任意用户 revoke 他人 lease，严格对照 etcd 测试。
- Auth 管理 RPC：Auth 未开启时允许 bootstrap 所需操作；开启后只允许 root 管理，
  用户修改自身密码的例外行为按 etcd 对齐。

## 实施阶段

1. **A0 差分矩阵**：固定未启用状态、bootstrap、用户/角色 CRUD、错误码和 header。
2. **A1 存储原语**：internal CAS batch、auth revision、恢复和并发 mutation 测试。
3. **A2 管理面**：User/Role/Permission/AuthEnable/Disable/Status，暂不宣称数据面可用。
4. **A3 token**：Authenticate、签名密钥恢复/轮换、旧 revision token 失效。
5. **A4 unary 数据面**：KV/Txn/Lease 权限；3 副本撤权一致性与 fail-closed 注入。
6. **A5 streaming**：Watch 创建、已有 watch 撤权、重连和慢消费者。
7. **A6 生产验证**：官方 client/etcdctl 双端差分、race、TiKV/PD 故障、Leader
   切换、全部 KubeBrain 重启、密码与 token 泄漏审计、持续权限 churn soak。

在 A4 和 A5 完成前，`AuthEnable` 必须继续返回 `Unimplemented`。禁止先开放管理
RPC 再依赖文档警告，因为客户端会把成功开启 Auth 视为数据已受保护。

## 明确不复刻的 etcd 内部实现

- 不移植 bbolt auth bucket；使用 keyspace 隔离的 TiKV internal KV。
- 不依赖 etcd Raft apply index；使用 internal CAS revision 和 auth event log。
- 不复制 simple token 的进程内随机 map；它无法跨 KubeBrain 副本和重启验证。
- 不把 DBaaS 控制面 IAM 当作 key-range permission。
