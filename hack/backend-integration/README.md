# 后端 TiKV 协议集成回归

本目录同时维护根模块的本机真实 PD/TiKV 测试入口，以及尚待替换的独立
unistore mock Go 模块。两者均不接入 Kubernetes，不需要 kubeconfig。

## 本机真实协议对照（新增，尚未替代 mock）

`backend-integration.yml` 新增独立 `real-protocol` 门禁：在可信 self-hosted
Linux amd64 Runner 上拉取入口使用的固定镜像摘要，运行构建契约、vet、
十例真实协议普通版与 race 版，以及两个启动阶段的中断清理测试。每次入口
使用全新的临时集群，不能用同一前缀的 `-count` 重复 Region 分裂测试。
此门禁不替代仍保留的 mock 门禁；本机通过不等同于 Runner 已验收。

首轮 Runner 验证在创建容器前失败：该 Docker 版本拒绝在自动分配子网的网络
上指定静态 IP。本机版本未拒绝，因此保留这次 CI 失败记录。入口现先由 Docker
选择子网，检查本次预留网络的所有权和空容器集合，按确切 ID 移除，再显式
指定相同子网和网关重建；并发分配冲突会失败退出，不复用或修改外部网络。
所有权变化、网络被占用、状态缺失、删除失败及重建冲突均有失败关闭契约。

第二轮 Runner 已越过静态 IP 创建错误，但在 90 秒就绪检查处超时，协议测试
尚未开始；该次日志报告清理成功。超时根因尚未确定，不能将其算作修复通过。
入口在就绪失败时输出最后一次 HTTP 错误、PD stores 响应和本次两个容器的
有限状态字段；失败退出清理时还输出各容器最后 100 行启动日志。没有放宽
就绪期限，不打印完整容器环境或宿主配置。

```sh
bash hack/backend-integration/run-real-local.sh --allow-local-containers
bash hack/backend-integration/run-real-local.sh --allow-local-containers --race
```

此独立入口仅连接本机 `/var/run/docker.sock`，需要 Linux Docker、Go、jq、
curl、openssl 和 timeout。预先准备脚本中固定摘要的 PD/TiKV 8.5.3 amd64
镜像；入口不自动拉取、不接受外部 PD 地址。它创建一个内部网络及各一个
PD/TiKV 容器，不发布宿主端口，以非 root 用户、只读根文件系统、受限 CPU/
内存和临时内存盘运行；`local-*.toml` 只用于此单副本临时环境，不能部署到
共享集群。单副本、内存盘测试不证明 Raft 多副本持久性或磁盘性能。

入口编译根模块的真实存储测试，分别运行 1PC、丢响应后默认重试、丢响应后
取消，以及后端对已提交/未送达结果的解析与默认重试。每例使用随机前缀和实际集群 ID；
退出时校验本次资源所有权再清理容器/网络，移除自己的编译二进制，保留
打印出的 `/tmp/kubebrain-real-protocol.*` 目录中的日志。失败清理会返回失败，
不能将未知状态视为资源已消失。扩展后的十例普通运行全部通过。
入口还要求每个指定用例输出对应名称的 PASS；SKIP、未匹配用例或
非零退出均不能通过。失败时输出最后 100 行用例日志，保留原始退出码及完整
本机日志；CI 控制台逐例打印确认标记。两个后端默认重试
场景均只重试一次、保持事务时间戳不变、两键只发布一个修订号/Watch 批次，
下一次写入只递增一个修订号。清理函数的 13 个情形和入口参数拒绝已用
无 Docker 的替身测试覆盖，`-race -count=10` 通过；包括所有权变化、查询失败、
资源仍存在、删除失败、日志导出失败、缺失网络状态及符号链接保护。日志失败
仍导致失败退出，但不妨碍删除独立确认归本次所有且为空的网络。
真实 Region 分裂用例在已分组的首次 1PC prewrite 发送前调用 SplitRegions，
要求实际 epoch 错误、多 Region prewrite、两阶段 commit 及相同事务/Watch
语义；额外的 `KUBEBRAIN_TIKV_PROTOCOL_ALLOW_REGION_SPLIT=1` 仅由隔离入口设置。
不要对共享集群运行此用例，删除测试键不会恢复 Region 边界。
启动中断可单独验证（同样需要预先准备固定镜像）：

```sh
bash hack/backend-integration/run-real-local-interruption-test.sh --allow-local-containers
```

该测试在本次 PD/TiKV 创建后分别向自己的入口进程发送 SIGTERM，检查退出码
143、资源实际不存在及编译二进制移除。两阶段实测通过。曾发现信号处理期间
清理标记被重定向进资源 ID 文件，现保留独立输出描述符，并用替身和真实中断
验证修复。它不测试 SIGKILL、Docker daemon 崩溃或宿主机失联。
mock 故障语义的完整替代审核尚未完成，不能据此
删除 mock 测试或宣称下述测试依赖告警已解决。

### 旧 mock 与真实测试的覆盖对照

以下名称均为根模块 `TestRealTiKVBackend` 后缀；不是仅检查进程退出码。

| 旧 mock 场景 | 真实用例后缀 | 必须保持的结果 |
| --- | --- | --- |
| 未送达、禁用 RPC 重试 | `NoRPCRetryUndeliveredOnePC` | 上下文仍有效，仅一次发送，不确定结果解析为未提交，候选修订号可复用 |
| 已提交、禁用 RPC 重试 | `NoRPCRetryCommittedOnePC` | 上下文仍有效，仅一次发送，不确定结果解析为已提交，两键同修订号和 Watch 批次 |
| 未送达、默认重试 | `RetriesUndeliveredOnePC` | 两次发送、相同事务时间戳、成功健康检查、只提交一次 |
| 已提交、默认重试 | `RetriesCommittedOnePC` | 两次发送、相同事务时间戳、成功健康检查、无重复提交或 Watch 批次 |
| 首次 prewrite 前分裂 | `RegionSplitFallback` | 真实 epoch 错误、至少两个 Region 成功 prewrite、两阶段 commit、原子修订号 |

默认重试两例在 TxnApply 前后读取 SDK 的 `StatusCountWithOK` 增量。当前固定
客户端版本仅在真实 gRPC Health/Check 返回 SERVING 后递增该计数；隔离入口
每例独立进程，不以伪造健康服务或成功的数据 RPC 数替代健康检查。补充断言后
十例真实 race 运行通过，两种默认重试均观测到一次成功健康检查。

有意保留的服务端差异：旧 unistore 在提交后丢响应并重试时仍返回不确定结果，
真实 TiKV 8.5.3 则确认成功。因此不能硬编码旧 mock 的返回错误作为真实服务端
规范；两者必须共同满足同一事务、单次持久提交和单次事件发布。真实“已提交但
调用返回不确定”的见证解析由禁用 RPC 重试且上下文仍有效的用例单独锁定。
取消上下文两例、协议 smoke 和原始丢响应测试是额外覆盖。CI 尚需验证，旧模块
及其漏洞告警仍保留；此对照不证明多副本故障恢复或生产性能达标。

根模块的 `govulncheck@v1.6.0 -test ./...` 已通过，`-show verbose` 结果为
0 个符号级、0 个导入包级漏洞；模块级仍报告未被这些代码导入的
`golang.org/x/crypto/openpgp` 告警 GO-2026-5932，不将其描述为完全没有模块告警。
`build` 中的依赖边界测试显式包含真实协议测试二进制，禁止重新引入整个
`github.com/pingcap/tidb` 包前缀。根模块扫描不覆盖下方独立 mock 模块，也不
消除它的 GO-2024-3284 告警。

新增的禁用重试两例不取消调用上下文，以对照旧 mock 的普通 RPC 错误场景。
只有显式测试环境变量允许时，测试进程才在 `TestMain` 一次性启用 SDK
failpoint 支持；每例在创建客户端之前设置 `noRetryOnRpcError`，关闭所有
客户端后撤销，不在后台工作期间反复写 SDK 的非原子总开关。默认测试不启用
该开关，普通重试用例检查自己未继承禁用重试状态。此配置不进入产品二进制。

当前安全检查限制：包含测试文件的 `govulncheck -test ./...` 会报告旧 TiDB
模拟依赖的 [GO-2024-3284](https://pkg.go.dev/vuln/GO-2024-3284)。该问题尚未
完成处理，不因它是测试模块就忽略，也不能省略 `-test` 得到空扫描结果。
数据面构建不包含此 TiDB 依赖；详见仓库的
[验收记录](../../docs/dbaas_acceptance_status_cn.md)。

```sh
bash hack/backend-integration/run-onepc.sh --count 10
bash hack/backend-integration/run-onepc.sh --race --count 3
```

入口固定 Go 1.26.8、只读模块解析、测试范围和 180 秒测试超时；先用同一临时
modfile 执行完整 vet，再编译并运行测试。只接受上面的选项
及 `--help`，次数范围为 1–100。环境需要 Bash、Go、jq、patch 和 GNU coreutils/find。
不传递任意测试参数，不读取真实集群端点。测试中的 1PC 开关仅作用于测试进程，
清理时恢复；产品配置没有改变。每种故障都分别测试禁用 RPC 重试和默认重试，
仅前者启用 `noRetryOnRpcError` failpoint，并在清理时撤销；后者验证它没有遗留。
客户端全局 failpoint 总开关在 `TestMain` 中、任何客户端创建前只初始化一次，
避免逐用例重复写入该非原子开关与上一用例的后台锁解析发生数据竞争。

覆盖提交前未送达、实际服务端 1PC 提交后响应丢失两种普通传输错误，检查：

- 客户端及适配器返回不确定结果，只有带用户上下文标记的提交消耗故障。
- 后端按持久见证解析为未提交或已提交，两键不会部分可见。
- 未提交不推进公共修订号，后续写入复用候选修订号；已提交后只递增一次。
- 已提交两键以同一修订号、同一 watch 批次发布；下一次确认写入之前没有额外事件。
  内部 API 区分 CREATE/PUT，对外 etcd 适配层将两者映射为 PUT。

默认重试用例在 loopback 随机端口启动真实 gRPC Health 服务，并给相同 mock StoreID
注册该地址，让客户端默认的健康检查和重试实际发生；数据 RPC 仍使用进程内 unistore。
用例验证健康检查被调用、恰好发送两次、事务起始时间戳不变、不会出现两个提交时间戳。
发送前故障在重试后返回成功；本版本 mock 中提交后丢响应在重试后仍返回不确定结果，
由后端见证解析为已提交。禁用重试场景只能发送一次。健康服务在后端关闭后停止并回收。

Region 分裂用例在带用户标记的第一次 1PC prewrite 发送前分裂 mock Region，
使已完成分组的请求携带旧 epoch。要求客户端保留事务起始时间戳，实际向至少两个
Region 成功 prewrite 并执行两阶段 commit，不能出现 1PC 提交结果。后端两键读取和
同一 watch 批次使用同一修订号，下一次写入只递增一次。这是受控客户端回退测试，
没有操作真实 TiKV Region，也不是真实 Raft 持久性或故障恢复证明。

`go.mod` 的客户端远程替换必须与根模块完全一致，入口会拒绝漂移；根模块只用
相对路径 `../..` 引用，不依赖 `/root/tikv-client-go` 或任何私有诊断目录。
旧 TiDB mock 依赖在 Go 1.26 下引用私有 runtime 符号，因此入口校验模块与源码摘要，
只在自己的临时副本中应用 `compat/tidb-printer-go126.patch`，改用公开 runtime API。
不修改模块缓存，不关闭链接器检查，不把 TiDB 源码引入产品。
维护客户端版本时同步两个模块并重新验证，不通过替换成本机副本绕过检查。

非 root 清理契约可独立运行：

```sh
bash hack/backend-integration/run-onepc-cleanup-test.sh
bash hack/backend-integration/run-onepc-entry-test.sh
```

清理契约检查嵌套只读目录清理、退出码保留和外部符号链接目标不被修改，禁止以 root 运行。
入口契约检查无效次数、真实端点参数、任意测试参数、客户端版本漂移及双方同时误用
本机替换路径时提前拒绝；它不运行 Go 集成测试，也不准备 mock 依赖。
临时目录前缀与客户端 fork 的 mock 入口相同，但每次创建独立随机目录，清理只针对
该次调用的确切路径。兼容补丁和清理策略来自客户端 fork 提交 `2155365`。

边界：这不是真实 TiKV Raft 持久性、完整 TiKV 网络协议、外部 etcd gRPC Watch、真实跨 Region
事务或 30 秒恢复／900 秒升级验收的证明，不能据此启用生产 1PC。
