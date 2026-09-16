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

## 真实后端持久见证解析

`pkg/storage/tikv/backend_uncertain_test.go` 的
`TestRealTiKVBackendResolvesCancelledOnePC` 启动进程内的真实 KubeBrain 后端，
连接真实 TiKV，复用上述响应丢失后取消请求的注入器。使用相同四个专用环境变量，
显式 `1pc`、全新前缀和独立进程，精确运行：

```sh
go test ./pkg/storage/tikv -run '^TestRealTiKVBackendResolvesCancelledOnePC$' -count=1 -timeout=120s -v
```

运行前仍需固定工具链、核对集群 ID，并遵守前面的 TLS、非特权 Pod 和日志保存要求。
测试从前缀的随机 nonce 派生独立 `protocol-backend-<nonce>` keyspace；写入前同时
检查原始前缀范围和派生物理对象范围为空，再原子创建随机所有权令牌。不得复用
其他 smoke 的前缀。后端保留存储适配器能力，但不启动选主、全局 GC 或自动压缩。

用例要求 `TxnApply` 真正返回 `ErrUncertainResult`；一次 prewrite、一次成功响应
丢失、非零真实提交时间戳；后端 committed 解析指标恰为 1、not_committed 为 0。
双键 CREATE 和读取必须使用同一修订号 101，后续单键 PUT 必须为 102，并作为有序
边界检查前面未重放双键事件。调用者取消不能取消后端独立的见证解析工作线程。

清理先调用 `backend.Close` 等待工作线程并释放检查点保护，再用独立存储客户端
执行所有权 CAS 和删除；不会用已关闭的客户端，也不通过隐藏能力接口跳过真实路径。
只扫描两个派生的隔离范围，最多允许 128 个键，超量报错不删除。所有权缺失但仍有
数据、所有权不符或检查后发生变化均拒绝删除；删除后重新检查两个范围为空。
关闭失败时保留数据及所有权供恢复。清理边界有本地内存存储测试，不等同真实故障测试。

成功要求 PASS、`PROTOCOL_BACKEND_RESOLVED` 和 `PROTOCOL_BACKEND_CLEANUP_OK` 齐全。
2026-09-11 专用集群实际通过，start/commit TS 为
`468999580939452418` / `468999580939452420`，revision=101、next=102。
这补齐了真实 TiKV 已提交分支的后台见证解析证据，不覆盖未提交分支、进程重启、
领导权切换、真实 Region 分裂、Raft 故障持久性或 900 秒升级验收；生产 1PC 仍关闭。

### 未送达分支

`TestRealTiKVBackendResolvesUndeliveredOnePC` 使用相同运行条件和清理路径，但在带标记
且 `TryOnePc=true` 的 prewrite 调用底层传输之前，返回注入错误并取消调用者。
普通 2PC、后台请求不消耗故障；本地测试核对底层调用数，确保目标请求确实没有送出。
这是本测试客户端的受控中断，不是实际集群网络分区，也不是服务端提交后回滚。

精确运行 `-run '^TestRealTiKVBackendResolvesUndeliveredOnePC$' -count=1 -timeout=120s`，
仍要求四个专用环境变量、独立进程和全新前缀。真实适配器必须返回
`ErrUncertainResult`，候选修订号 101、尝试数 1、拦截数 1、提交时间戳 0。
后台解析必须为 not_committed=1、committed=0；可见修订号保持 100、双键不存在。
下一次单键写入必须复用 101，且第一个 watch 变更只能是这次 CREATE，不能先收到
被中断双键事务的事件。成功仍需 PASS、RESOLVED 和 CLEANUP_OK 三项齐全。

2026-09-11 实测通过：start TS=`468999762448220169`，commit TS=0，next=101。
同一构建在另一新前缀复验已提交分支也通过：start/commit TS 为
`468999782213615623` / `468999782213615625`，next=102。两次均完成所有权清理。
证据目录 `/root/.local/state/kubebrain/real-backend-absent.Ejf6ofNf/`。
这补齐上述受控取消方式下的两种解析结果，不改变进程崩溃、跨 Region、Raft 故障、
性能或升级验收仍需独立证明的要求。

## 固定后端更新的协议延迟测量

新增 `TestRealTiKVBackendAsyncProtocolLatency` 与下述用例使用同样的数据与计时边界。
必须显式设置 `KUBEBRAIN_TIKV_PROTOCOL_ASYNC_EXPERIMENT=1`，模式仍设为 `2pc`：
进程默认、所有权声明及清理保持 2PC，仅预热和测量的用户更新逐事务启用 async、关闭 1PC。
每笔测量必须恰有一次 async 请求和有效接受响应，且没有 1PC 成功或 async 回退；
该断言只适用于无重试的单 Region fixture，不可外推为多 Region 事务协议判定。
后台 commit 可能继承计数标记，因此不固定其次数，也不把其耗时当成前台提交耗时。
对照顺序为独立进程的 2PC → async → async → 2PC，每组使用新前缀。
本地 Docker runner 纳入此用例用于真实协议验证；其单副本 tmpfs 耗时不是测试集群性能证据，
更不能替代 6000 次操作 / 900 秒的滚动验收。本段描述测试方法，不声明性能改进已获验证。

2026-09-16 本地隔离 runner 的完整 race 执行退出 0，`cleanup_failed=0`；
新增用例的 20 次 prewrite 全部接受 async，1PC/回退/错误均为 0，Watch、最终读取及配额检查通过。
独立检查确认此次所有权标签对应的容器、网络和编译测试文件均已清理。
原始日志持久保存在 `/root/.local/state/kubebrain/async-latency-protocol.QKiWqZPzaw`。
另行通过 storage/tikv 全包 race（8.029 秒）、build race（2.529 秒）及 `go vet ./pkg/storage/tikv`。
这证明新增测量夹具可执行，不证明专用集群 ABBA 对照或滚动验收已经完成。

带保护的对照入口为 `TestRealTiKVBackendFencedProtocolLatency` 和
`TestRealTiKVBackendAsyncFencedProtocolLatency`。二者先建立真实领导权／恢复
存储 fence，再执行相同的创建、10 次预热与 20 次更新；仅后者需要 async opt-in。
每组仍使用独立所有权和有界清理，额外允许的键仅为原有 512 个明确命名的保护分片。
每笔记录实际 prewrite 分组／RPC 数，而不把生产形态强行认作单 Region；async
要求该笔全部 prewrite 响应接受，且零错误／零回退。2PC 的后台 secondary commit
计数不固定，前台 primary 成功由逐批 observer 核验。用例不主动切分共享集群 Region。
同样按独立进程 ABBA 比较；这仍是后端测量，未包含公共 gRPC、鉴权、选主滚动及
客户端 pacing，不能替代完整的 900 秒验收。不得混合有保护与无保护的样本作协议归因。

2026-09-16 带保护版本的本地完整真实 TiKV race runner 退出 0，清理错误为 0。
两个新增用例各 20 笔更新均观测到两类保护写入；本轮各笔为 2 个 prewrite 分组，
async 共 40 个请求／接受响应、无回退／错误。这里不是三副本存储或正式 ABBA 数据。
storage/tikv 全包 race 7.865 秒、build race 2.499 秒、vet 通过；新增内存夹具上界
测试证明 31 次带保护写入仍在既有清理预算内。独立检查此次临时容器、网络和
测试二进制均已清理；日志保存于 `/root/.local/state/kubebrain/fenced-latency-protocol.OgrdZHF5jF`。

`TestRealTiKVBackendProtocolLatency` 复用相同显式 PD/集群 ID/全新前缀/模式要求及
两范围所有权清理。精确选择用例，`-count=1 -timeout=120s`，每组独立进程运行。
固定条件为 2 GiB 配额、单键 256 字节值、一次创建、10 次预热、20 次测量更新；
只计时 `backend.TxnApply`，逐次 watch 核验、最终读取和配额核验在计时段之外。
不注入故障、不启用后台全局 GC/自动压缩，也不启动公共 gRPC 或选主。

同一二进制、同一 Pod 内按 2PC → 1PC → 1PC → 2PC 顺序运行，每组另用新前缀。
测量请求携带独立上下文标记：预热、后台检查点和清理不计入 RPC 计数；每组必须
恰有 20 次成功 prewrite，1PC 有 20 个成功提交时间戳且无 commit RPC，2PC 有
20 个 commit 成功响应且无 1PC 时间戳。故障、重试或协议回退不满足该受控样本条件。
后台工作仍可能争用资源；标记只隔离计数，不隔离负载。输出原始 `durations_ns`。

该固定规模的物理键数量有本地上界回归检查，并保留真实检查点/所有权元数据余量；
仍沿用 128 键清理硬上限。不要直接增加样本数而不重新验证清理容量和时间预算。
成功需要每组 PASS、`PROTOCOL_BACKEND_LATENCY` 和 `PROTOCOL_BACKEND_CLEANUP_OK`。

2026-09-11 同一 `k8s3-compute1` Pod 的四组实测通过：

| 顺序 | 模式 | 样本数 | 平均耗时（ms） |
| --- | --- | --- | --- |
| 1 | 2PC | 20 | 46.85 |
| 2 | 1PC | 20 | 28.31 |
| 3 | 1PC | 20 | 30.50 |
| 4 | 2PC | 20 | 45.00 |

合并后各 40 个样本，平均值为 45.92 / 29.41 ms，差约 16.52 ms。原始值、协议计数、
构建信息和清理证据见 `/root/.local/state/kubebrain/backend-latency.dNiFb5M6/`。
这是小样本、单 Region、后端内部更新的探索性比较，不是 p99/SLO，也不覆盖认证、
公共 RPC、代理或副本 watch。不可直接与旧混合流量的 Put 均值相减，推断 RPC 开销。
不能据此启用生产 1PC 或认定 900 秒升级门禁会通过；下一步需相同工作负载下的
端到端测量及真实跨 Region/故障验证。
