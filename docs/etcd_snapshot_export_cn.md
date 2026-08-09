# 导出可由官方 etcd 恢复的快照

KubeBrain 的在线存储是独立 TiKV/PD，不存在可直接复制的本地 etcd bbolt backend。当前
`Maintenance.Snapshot` 会在固定 revision 上以有界 chunk 扫描用户 keyspace，增量生成带
SHA-256 尾部的 etcd 3.7 backend snapshot；root 可以直接使用官方 etcdctl：

```bash
ETCDCTL_API=3 etcdctl --endpoints=http://kubebrain:3379 snapshot save snapshot.db
etcdutl snapshot status snapshot.db -w json
etcdutl snapshot restore snapshot.db --data-dir restored.etcd
```

在线快照保留 KV MVCC history、真实 compact watermark、当前 lease、auth 用户/角色/修订和
alarm。旧的非内联数据布局没有逐历史版本 lease 字段，因此只能由固定在同一线性化点的
durable key→lease attachment 精确恢复当前版本；仍保留且无法判定 lease 的旧历史版本会让
snapshot fail closed，物理 Compact 清除这些含糊版本后才恢复可用，禁止伪造 lease=0。
lease 的倒计时按官方 etcd 的持久 checkpoint 语义恢复，而不是逐秒保存抓取瞬间的实时 TTL；
auth token 会像官方 etcd 重启后一样失效，客户端必须用保留的用户凭据重新认证。输出仍不
替代 TiKV 物理 PITR。
在线 Snapshot 必须在当前 leader 上捕获。开启 etcd proxy 时，follower 会透明转发完整 stream；
关闭 proxy 时，follower 返回 `ErrGRPCNotLeader`，由客户端改选 leader。不能在 follower 本地生成：
`BeginRangeTxn` 只冻结当前进程的逻辑写，无法阻止另一 Pod 的 leader 在多次 TiKV metadata 读取之间
提交变更，否则 KV revision 与 auth/lease/alarm 可能来自从未同时存在的状态。

对于已有 `kubebrain.logical.v2` 逻辑制品，仍可使用离线转换路径：

```bash
ENDPOINT=http://kubebrain:3379 \
PREFIX=/ \
OUTPUT=kubebrain-full.jsonl \
BATCH_SIZE=1000 \
  hack/backup/logical-export.sh

kubebrain-logical-etcd-snapshot \
  --input kubebrain-full.jsonl \
  --output snapshot.db \
  --acknowledge-auth-disabled

etcdutl snapshot status snapshot.db -w json
etcdutl snapshot restore snapshot.db --data-dir restored.etcd
```

转换器只接受：

- 当前 `kubebrain.logical.v2` 格式；
- 精确的全 keyspace 前缀 `/`，避免把局部备份伪装成完整 etcd snapshot；
- 每个 lease 都含 `granted_ttl`；
- create/mod revision、version 和 snapshot revision 之间关系可成立的记录；
- 操作者显式传入 `--acknowledge-auth-disabled`。

输出保证：

- 官方 3.7 backend schema、完整 bbolt bucket 和文件 SHA-256；
- snapshot revision 必须为正并与 KubeBrain 导出点一致，包括巨大的 TiKV TSO revision；revision 0
  不会静默规范为官方初始 revision 1，而是在创建制品前拒绝；
- snapshot revision 必须小于 `MaxInt64`，为恢复后承诺的下一次官方 etcd 写入保留一个 signed
  revision；`MaxInt64-1` 仍是可导出的上界；
- 当前 key/value、create revision、mod revision、version 和 lease ID 保持一致；
- lease 的 granted TTL、导出时剩余 TTL 与 attached keys 可恢复；
- 下一次官方 etcd 写入从 `snapshot revision + 1` 开始；
- snapshot 点之前的历史明确视为已压缩，而不是伪造不存在的历史版本；
- 输出使用原子 no-clobber 发布，并拒绝覆盖既有文件。

限制：

- 逻辑制品不包含密码哈希、auth revision 或 token signing state，因此输出 snapshot 强制
  `auth disabled`；不得把它当作保留 KubeBrain 认证配置的迁移方式。
- member、Raft/WAL、cluster ID 由 `etcdutl snapshot restore` 为目标集群重新生成。
- 这条转换命令是离线迁移/导出路径，不是 PITR、增量备份或 TiKV 物理灾备的替代品；需要
  保留 auth 配置时应优先使用在线 `Maintenance.Snapshot`。
- `kubebrain.logical.v2` 本身仍不能直接交给 `etcdutl`；必须先通过转换器。

发布门禁应至少包含：转换器单元与 race 测试、官方 snapshot status、官方 restore、恢复后
etcd 启动、当前 KV 元数据对比、`snapshot+1` 写入 revision、compacted 边界，以及带 lease
样本的 ID/TTL/attached-key 验证。

在线历史 snapshot 还要求每个 upstream MVCC `(main revision, subrevision)` 物理身份唯一。若
scanner 输入在同批或跨批重复该身份，builder 必须原子拒绝，不能依赖 bbolt `Put` 静默覆盖并
发布少事件的可恢复制品；已成功提交的早期批次保持原样，失败批次不写入。
对严格大于 compact watermark 的 ordered history，同一 main revision 的 subrevision 还必须从 0
连续递增；无 watermark 时检查全部 ordered history。watermark 及之前允许只保留 compaction 所需
的单 key 锚点，不能把合法的旧 subrevision 缺口误判为丢事件。连续性在 Finish 跨全部 batch 校验，
失败后可继续 Append 缺失记录再完成私有制品。
完整历史中的同一 main revision 也不能混合真实 ordered subrevision 与 `2^32+` legacy fallback：
现代事务的 event metadata 与对象写入原子提交，混合意味着至少一条 ordering evidence 已丢失或损坏。
全 legacy revision 仍可稳定 fallback；compact watermark 及之前的不可查询 anchor 不应用此限制。
每条 ordered record 还必须携带 event payload 重复保存的 `totalChanges`，并满足
`0 <= subrevision < totalChanges`。同一 main 的 total 必须一致；watermark 之后实际连续记录数必须
精确等于 total，以同时发现内部缺口和缺失尾部。watermark 内只校验单条 envelope 与 repeated total
一致性，不要求 compaction anchor 数等于原事务总数。
PreserveHistory 还会按物理 revision 重建每个 key 的 generation。compact watermark 及之前的最后
记录可作为已压缩 anchor；之后每次 Put 必须保持 create revision 且 version 精确 +1，tombstone 必须
终止一个 live generation，下一次重建必须满足 `createRevision=modRevision, version=1`。这样既不要求
已压缩版本仍存在，也不会发布可启动但 protobuf KV metadata 与恢复索引不一致的历史。
严格大于 compact watermark 时，同一 main revision 也只能修改同一 key 一次；公开 Txn 会拒绝
Put/Put、Put/DeleteRange 的重叠，重叠 DeleteRange 的后续操作也不会再次产生已删除 key 的事件。
metadata bucket 同样要求 lease ID、auth username、role name 和 alarm `(member ID, alarm type)`
分别唯一；重复身份即使内容完全
相同也视为上游状态不自洽并拒绝整个 metadata transaction，不能让 TTL、用户角色或权限由 slice
顺序决定。Alarm 的 bbolt key 虽是完整 protobuf，逻辑身份仍按 tuple 去重，避免不同 unknown fields
形成多个物理 key并在解除后重启复活；同一 member 同时携带 NOSPACE/CORRUPT 是合法状态，不按
member ID 错误去重。
Alarm type `NONE` 不能作为 alarm record 持久化：它只表示“查询全部 active alarms”的公开 API
sentinel。upstream ACTIVATE(NONE) 是不落库的 no-op；若制品直接写入 NONE，官方 AlarmStore restore
后反而会在 GET(NONE) 的全类型枚举中把它作为真实告警返回。未知非零 enum 仍按 upstream 行为保留。
Auth snapshot 还必须满足上游管理 API 可达的不变量：每个用户的 role 列表不重复、按字符串字节序
排序，且除特殊 `root` role 外全部引用已存在 role record；upstream 与 KubeBrain 都允许直接把
`root` 授给用户而不创建同名 role record。`authEnabled=true` 时必须存在 root 用户，且该用户持有
root role。违反任一条件都会回滚完整
metadata transaction，避免恢复出无法通过正常 AuthEnable/RoleDelete 路径产生的权限状态。
User/Role name 还必须是非空合法 UTF-8：backend 虽以 bytes 持久化，公开 Auth 请求及 List 响应使用
protobuf string，非法 UTF-8 会令恢复后的管理响应无法 marshal。不会额外执行 Unicode normalization。
每个 role 的 permission 必须非空指针、符合 etcd permission range 规则、按 key 排序，且同一
`(key, range_end)` 只能出现一次；相同范围的再次 grant 是权限类型更新，不是第二条记录。未知
permission enum 与上游一样允许持久化但不授予 READ/WRITE，不能擅自把它当作非法 range 拒绝。
`UserAddOptions.NoPassword=true` 的用户不得携带 password bytes；普通或 legacy 用户的 password
字段保持 opaque bytes。上游 `HashedPassword` 路径只做 base64 decode，并不验证 bcrypt，因此
snapshot writer 也不能擅自拒绝非 bcrypt 内容。
Auth revision 必须能够覆盖当前对象图所需的最少管理 mutation：下界为
`1 + user 数 + role 数 + user-role 边数 + permission 数`。空、禁用且无对象时允许 revision 0，
它表示 upstream AuthStore 尚未初始化的 sentinel，恢复启动后会被持久化为 1；任何包含对象或启用
状态但低于图下界的制品都会被拒绝，避免恢复出 token/permission fence 与管理历史不一致的状态。
Auth revision 还必须小于 `MaxUint64`：upstream 每次 auth mutation 直接执行无溢出检查的
`atomic.AddUint64`，因此精确最大值会令恢复后的下一次合法变更回绕到 revision 0。`MaxUint64-1`
仍合法，并允许官方 AuthStore 再执行一次 mutation 到达最大值。
Builder 在 Finish 时还会按物理 revision 顺序重建每个 key 的最终 lease 引用；最终非零 lease ID
必须存在于 lease bucket。历史版本引用后来已撤销的 lease 是合法的，只要该 key 随后被无 lease
版本覆盖或 tombstone 删除；不能把当前引用完整性误扩大成所有历史 lease 都必须保留。
Lease metadata 要求 `0 <= remainingTTL <= grantedTTL`：0 表示没有有效 checkpoint，等于 granted
可出现在刚 grant/renew 的导出点，大于 granted 会非法延长恢复后的租约。显式 lease ID 仍与
upstream 一样允许负数，只禁止 0。grantedTTL 还不得超过 upstream `MaxLeaseTTL=9,000,000,000`
秒；官方恢复路径不会替 artifact 重做该上界校验，超界值可能在 expiry 时间运算中溢出。
