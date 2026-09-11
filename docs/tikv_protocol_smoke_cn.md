# 真实 TiKV 提交协议 smoke

入口为 `pkg/storage/tikv/protocol_smoke_test.go` 中的 `TestRealTiKVProtocolSmoke`。
这是显式启用的存储适配器测试，不启动 KubeBrain 服务，不改变现有服务的提交协议。
未设置专用 PD 环境变量时会跳过；跳过不算真实后端验证通过。

## 运行条件与范围

- 仅用于已授权的专用测试 TiKV/PD 集群。当前入口使用 `Security{}`，不适用于要求
  客户端 TLS 的后端；不要为了运行它关闭现有集群 TLS。
- 测试进程须能连接 PD，以及 PD 返回的 TiKV 地址。单独转发 PD 端口通常不够。
- 预先独立核对 PD 集群 ID；不得把刚连接到的任意集群 ID 自动当作预期值。
- 每次使用全新前缀 `kubebrain/protocol-smoke/<32 位小写十六进制>/`，先持久保存
  该前缀和运行日志，禁止使用业务前缀或复用未完成清理的前缀。
- 精确选择这个用例，使用 `-count=1`，每个模式独立进程运行，不能与其他写入测试
  混跑。客户端协议计数器是进程全局变量。

从能访问测试集群的环境，在仓库根目录执行。下面的端点与 ID 必须由操作者填入
并核对；`PROTOCOL_PD` 和 `PROTOCOL_CLUSTER_ID` 未设置时命令直接停止。

```sh
set -e
: "${PROTOCOL_PD:?填写已核验的测试 PD 地址，多个地址以逗号分隔}"
: "${PROTOCOL_CLUSTER_ID:?填写独立核验的预期 PD 集群 ID}"
for mode in 2pc 1pc; do
  nonce=$(openssl rand -hex 16)
  prefix="kubebrain/protocol-smoke/$nonce/"
  printf 'mode=%s prefix=%s\n' "$mode" "$prefix"
  env GOWORK=off GOTOOLCHAIN=go1.26.8 GOFLAGS=-mod=readonly \
    KUBEBRAIN_TIKV_PROTOCOL_PD="$PROTOCOL_PD" \
    KUBEBRAIN_TIKV_PROTOCOL_CLUSTER_ID="$PROTOCOL_CLUSTER_ID" \
    KUBEBRAIN_TIKV_PROTOCOL_PREFIX="$prefix" \
    KUBEBRAIN_TIKV_PROTOCOL_MODE="$mode" \
    go test ./pkg/storage/tikv -run '^TestRealTiKVProtocolSmoke$' \
      -count=1 -timeout=120s -v
done
```

也可预编译此包的测试二进制，在单独的非特权测试 Pod 内以相同环境变量和精确
`-test.run` 参数运行。核对上传前后摘要，禁止挂载宿主机目录或 API 令牌，限定资源、
临时磁盘及生命周期；测试结束后按 Pod UID 删除，仅清理本次构建的二进制。

## 断言与清理

写入前检查参数、实际集群 ID 和空前缀。首次事务原子创建随机所有权令牌及
`data`、`witness` 两键；提交成功计数增量必须恰为所选协议一次，异步提交为零。
随后更新两键，验证固定 TiKV 时间戳仍读到旧值，而当前读取看到新值。
`witness` 只是本用例的第二个普通键，不是 KubeBrain 后端的不确定提交见证实现。

清理使用独立的 30 秒上下文，读取所有权并在删除事务内再次 CAS 比较，只删除本次
三个确切键，最后验证前缀为空。所有权不符、所有权在检查后变化、所有权缺失但仍有
数据等情况均报错并保留数据，不做宽泛范围删除。正常清理允许重复执行。
本地 `TestProtocolSmokeCleanupOwnership` 覆盖这些边界；移除清理 CAS 的反向用例
会在所有权竞争场景失败。

成功要求测试 PASS、`PROTOCOL_SMOKE_OK` 及 `PROTOCOL_FIXTURE_CLEANUP_OK` 都存在。
若进程被强杀或 cleanup 失败，不得只因 smoke 标记存在就宣布本次完成。保留 Pod、
前缀及 `PROTOCOL_FIXTURE_STARTED` 的 owner SHA256 记录，先核对实际集群和所有权，
再使用事务内所有权比较进行针对性恢复；不盲目重跑或删除整个前缀。

## 证据边界

一次 smoke 不证明真实 Region 分裂回退、响应丢失处理、Raft 故障持久性、
KubeBrain 修订/watch/不确定结果解析或滚动升级可用性。首次事务耗时包含冷启动等
因素，单次不同模式的数值不是受控性能对比。不能以此启用生产 1PC/async commit。

## 真实 1PC 成功响应丢失

`TestRealTiKVOnePCResponseLoss` 复用上述集群 ID、独占前缀、所有权与清理约束，
要求 `KUBEBRAIN_TIKV_PROTOCOL_MODE=1pc`。使用全新前缀，单独进程精确运行：

```sh
go test ./pkg/storage/tikv -run '^TestRealTiKVOnePCResponseLoss$' -count=1 -timeout=120s -v
```

运行前仍须设置上述四个专用环境变量及固定 Go 工具链。未提供 PD 时跳过，不算通过。
注入器仅包装本测试的 TiKV 客户端，不改集群网络或其他客户端。它先收到带非零
`OnePcCommitTs` 的成功 prewrite 响应，再对带上下文标记的首次用户提交返回普通
传输错误；后台请求、不成功的响应及普通两阶段 prewrite 不消耗故障。
故障只触发一次，保留默认 RPC 重试，检查重试的起始及提交时间戳不发生变化。

客户端可以重试后确认成功，也可以返回 `ErrUncertainResult`，但不能将已证实提交
误报为确定失败；随后必须从新的固定快照读到两键，再检查更新和历史值。
除 smoke/cleanup/PASS 外，还必须存在 `PROTOCOL_RESPONSE_LOSS_CONFIRMED`，记录实际
丢失次数和返回分支。成功重试的结果不代表测试执行过不确定结果解析分支。
此测试仍只覆盖存储适配器，第二键不是 KubeBrain 后端的持久见证；真实后端见证
解析、发送前故障、跨 Region 和 Raft 故障持久性需要其他测试。

## 成功响应丢失后取消调用者

`TestRealTiKVOnePCCancelAfterResponseLoss` 使用同样的四个专用环境变量、全新前缀和
独立进程约束，要求显式 `1pc`。精确选择该用例，`-count=1 -timeout=120s`。
注入器收到真实成功提交响应后，丢弃响应并取消该次提交的子上下文；不取消后续
读取、清理或后台客户端上下文，也不启用全局禁止重试 failpoint。

本用例必须返回 `storage.ErrUncertainResult`，且恰有一次 prewrite 尝试、一次响应
丢失及非零成功提交时间戳；若重试确认成功或只返回确定的取消错误，测试失败。
随后用仍有效的独立上下文检查两键可见、历史快照不变，并执行相同所有权清理。
客户端成功提交计数可能为零：它未收到确认，不能以该计数否定服务端实际提交。

2026-09-11 专用集群实测通过：`uncertain=true attempts=1 drops=1`，
起始/提交时间戳分别为 `468999354656751617` / `468999354656751619`；
快照读取、更新和清理均通过。本证据证明存储适配器对这一取消边界的分类，仍不
证明 KubeBrain 后端持久见证解析、跨 Region 或 Raft 故障恢复。
