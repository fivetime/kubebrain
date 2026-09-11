# 后端 TiKV 协议集成回归

本目录维护根模块的本机真实 PD/TiKV 测试入口；旧独立 SQL mock 模块已退役。
不接入 Kubernetes，不需要 kubeconfig。

## 本机真实协议与 CI 门禁

`backend-integration.yml` 新增独立 `real-protocol` 门禁：在可信 self-hosted
Linux amd64 Runner 上拉取入口使用的固定镜像摘要，运行构建契约、vet、
十一例真实协议普通版与 race 版，以及两个启动阶段的中断清理测试。每次入口
使用全新的临时集群，不能用同一前缀的 `-count` 重复 Region 分裂测试。
主 CI 通过仓库内可复用工作流调用同一门禁，旧 mock 门禁已移除。
Runner 在 `a5f8e0bd` 的作业 `103343113136` 已完成：普通/race 各十项
明确 PASS，两阶段启动中断清理通过；完整日志已留存，不将单副本临时测试
描述为生产性能或多副本持久性验收。

新增 `TestRealTiKVReadBypassesPendingSecondaryCleanup` 单独使用 `2pc` 模式：
在隔离集群分裂两个键的 Region，真实提交主键，客户端仅暂停该事务的次要键
Commit 和后台 ResolveLock。读取仍返回已提交值，且观察到主键状态查询。
这验证后台锁清理无需阻塞该读取，不是证明锁清理没有负载成本；不能将 SDK
ResolveLock 总耗时直接归入前台 Put/Range 延迟，也不能据此启用生产 1PC。
2026-09-11 本地普通/race 各十一例通过，独立检查临时容器、网络、编译测试
文件均已清理；新增用例的远端 CI 结果需另行核验，不沿用上面的十例历史结果。

首轮 Runner 验证在创建容器前失败：该 Docker 版本拒绝在自动分配子网的网络
上指定静态 IP。本机版本未拒绝，因此保留这次 CI 失败记录。入口现先由 Docker
选择子网，检查本次预留网络的所有权和空容器集合，按确切 ID 移除，再显式
指定相同子网和网关重建；并发分配冲突会失败退出，不复用或修改外部网络。
所有权变化、网络被占用、状态缺失、删除失败及重建冲突均有失败关闭契约。

第二轮 Runner 已越过静态 IP 创建错误，但在 90 秒就绪检查处超时，协议测试
尚未开始；该次日志报告清理成功。当时根因未确定，不能将那次运行算作通过。
入口在就绪失败时输出最后一次 HTTP 错误、PD stores 响应和本次两个容器的
有限状态字段；失败退出清理时还输出各容器最后 100 行启动日志。没有放宽
就绪期限，不打印完整容器环境或宿主配置。

诊断版 Runner 随后确认根因：PD 可访问且仍在运行，TiKV 因容器文件描述符
上限 65,536 低于其启动要求 123,880 而以退出码 1 退出（非 OOM）。入口现给
本次临时容器显式设置 `--ulimit nofile=262144:262144`，不修改宿主 sysctl、
不启用 privileged，也不改变真实 Kubernetes 部署。修复已通过上述 Runner 验证。

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
下方列出保留下来的行为覆盖及有意接受的真实服务端差异。

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
取消上下文两例、协议 smoke 和原始丢响应测试是额外覆盖。上述 Runner 已验证
这些用例；此对照不证明多副本故障恢复或生产性能达标。

根模块的 `govulncheck@v1.6.0 -test ./...` 已通过，`-show verbose` 结果为
0 个符号级、0 个导入包级漏洞；模块级仍报告未被这些代码导入的
`golang.org/x/crypto/openpgp` 告警 GO-2026-5932，不将其描述为完全没有模块告警。
`build` 中的依赖边界测试显式包含真实协议测试二进制，禁止重新引入整个
`github.com/pingcap/tidb` 包前缀。旧独立模块已移除，根模块扫描保留 `-test`，
不以源文件扫描替代测试代码扫描。

新增的禁用重试两例不取消调用上下文，以对照旧 mock 的普通 RPC 错误场景。
只有显式测试环境变量允许时，测试进程才在 `TestMain` 一次性启用 SDK
failpoint 支持；每例在创建客户端之前设置 `noRetryOnRpcError`，关闭所有
客户端后撤销，不在后台工作期间反复写 SDK 的非原子总开关。默认测试不启用
该开关，普通重试用例检查自己未继承禁用重试状态。此配置不进入产品二进制。

旧 SQL mock 的独立模块、兼容补丁和三个运行入口已退役；历史源码可从
`a5f8e0bd` 的本目录恢复，不保留第二个可误用的当前入口。旧模块扫描曾检出
GO-2024-3284，处理方式是完成真实协议替代后移除依赖，不是漏洞豁免。
历史失败及替代证据见 [验收记录](../../docs/dbaas_acceptance_status_cn.md)。
