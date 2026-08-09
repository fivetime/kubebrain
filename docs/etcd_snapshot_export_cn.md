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
- snapshot revision 与 KubeBrain 导出点一致，包括巨大的 TiKV TSO revision；
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
metadata bucket 同样要求 lease ID、auth username 和 role name 分别唯一；重复身份即使内容完全
相同也视为上游状态不自洽并拒绝整个 metadata transaction，不能让 TTL、用户角色或权限由 slice
顺序决定。Alarm 以完整 `(member ID, alarm type)` protobuf 为 key，同一 member 同时携带
NOSPACE/CORRUPT 是合法状态，不按 member ID 错误去重。
Auth snapshot 还必须满足上游管理 API 可达的不变量：每个用户的 role 列表不重复且全部引用已存在
role，并按字符串字节序排序；`authEnabled=true` 时必须存在 root 用户，且该用户持有 root role。违反任一条件都会回滚完整
metadata transaction，避免恢复出无法通过正常 AuthEnable/RoleDelete 路径产生的权限状态。
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
