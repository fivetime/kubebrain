# 首个可验收版本：范围确认与当前阻塞

状态：2026-09-28 用户已确认六类首版必验清单及长租约清理规则调整；尚未全部通过。
尚未量化的项目仍需另行确认，不默认纳入或排除。已重新核验并恢复过期 TLS 证书。
执行约束见 [AGENTS.md](../AGENTS.md)。本页不扩充既有门限，不代替原始证据。

## 初检状态（04:52 UTC；后续恢复结果见下文）

- 本轮初检时本地 HEAD 为 `a97789ad`；最近成功的镜像/回归 CI 对应
  `1e862c110a82081e68ffa9612de4893d74decbc6`，运行分别为
  [35508057889](https://github.com/fivetime/kubebrain/actions/runs/35508057889) 和
  [35508057890](https://github.com/fivetime/kubebrain/actions/runs/35508057890)。
  后续三个本地提交未获这些 CI 覆盖；本轮没有构建或发布新候选。
- `/root/etcd` 当前提交为 `5cd9f4ee13801e18825d661e5005ae599460bc3a`。
  正式运行前还须固定所用二进制与 API/客户端版本，不能只引用源码目录。
- 专用命名空间 `kubebrain-dbaas-test` 中，`kubebrain-local` generation/observed
  为 138/138，镜像仍为 `sha256:50b9938fe3e5ad379136957438abe3c2d4bf250dc6c5973c869e0ef1341d1537`。
  04:52 UTC 初检仅 1/3 Ready；随后证书恢复后已为 3/3 Ready，见下。
- 本地 PD/TiKV 各 3/3 Ready；12 个 `kubebrain-local-lvm` PVC 均 Bound。
  这些状态不等于后端性能或数据完整性验收通过。
- 四个 Secret 的公开叶证书均已过期：client 为 9 月 23 日 21:48:19 UTC，
  peer 为 21:45:05 UTC，probe/info 为 21:45:06 UTC。
  `kubebrain-local-0` 在 9 月 28 日 04:52 UTC 的日志明确报告连接 `-2:3380`
  因 peer 证书过期而 TLS 握手失败。这是当前可用性的直接阻塞证据。
  peer Secret 的 CA 则有效至 10 月 16 日 21:45:05 UTC，无需本次更换信任根。
- 本次命名空间查询未发现 CiliumNetworkPolicy、Job 或故障 owner ConfigMap；
  仍有原隔离 NetworkPolicy。此检查不等于全面证明没有实验残留，执行前还须
  检查目标 Pod 标签和原实验状态。只读核验后仅进行了以下叶证书维护，未注入故障。

## 已发布候选身份（已完成下述控制面单项验收）

2026-09-28 重新查询 GitHub：上述镜像 CI 与回归 CI 均为 attempt 1、
completed/success，精确对应源码 `1e862c110a82081e68ffa9612de4893d74decbc6`。
后续正式运行以这份已发布候选为准备对象，不因文档或未接入的工具变更反复构建。

| 身份 | 固定值 |
| --- | --- |
| 发布制品 | `dbaas-release-35508057889-1`，artifact ID `10604608878` |
| 镜像 | `ghcr.io/fivetime/kubebrain@sha256:8aa38da6669729ecff864008e5e92df51be754f59e138a7b32424d1fca11589c` |
| linux/amd64 manifest | `sha256:618d18d9151d14c287d594fd246422905595fc19ef6e3b43839378c27237a84d` |
| linux/arm64 manifest | `sha256:b55d6ecc92a0c2ff4af3384166d7a8f2fe503686adcb8cf4685b2e6851a9b443` |
| 客户端 fork 依赖 | `github.com/fivetime/tikv-client-go/v2 v2.0.8-0.20260909023231-832b70fd622f` |

发布 ZIP 中 `release.json` 记录的仓库、源码、run/attempt 与上述 CI 一致，
`index.json` 摘要与镜像摘要一致，已归档的 registry index 同样匹配。
私有证据目录 `/root/.local/state/kubebrain/release-1e862c11-20260928.OiagU9uG/`
中的 `SHA256SUMS` 本轮全部校验通过。

本轮初检源码 `a97789ad` 比发布源码多三个工具提交；对比 `pkg`、`cmd`、
`go.mod`、`go.sum`、`Dockerfile`、`build` 无差异，但不能据此宣称这三个
提交已获得发布 CI 覆盖。正式报告必须分别记录产品版本和实际测试入口版本。
进一步核对构建作业 `106071088085` 的原始日志：CI 按上述两个平台摘要
拉取镜像，执行镜像内 `kube-brain version`，amd64/arm64 均输出精确源码
`1e862c110a82081e68ffa9612de4893d74decbc6` 和对应架构；amd64 完整输出
版本 `0.0.0-dbaas-1e862c110a82`、Storage=TiKV、Go=go1.26.8、
BuildTime=`2026-09-20T11:31:44Z`。`Verify published test image` 步骤成功，
对应工作流与发布提交无差异。这是 CI 内实际执行证据，不仅是标签声明。
原始日志：`/root/.local/state/kubebrain/candidate-image-identity.VcVclCFk/image-job.log`，
SHA-256 `25bb6a8324962ac6c24664a3321a722b0c003abab184ae260bae159e93ee45b9`。

上述发布核验时，本机未拉取或运行该候选。随后在专用集群完成预拉取、运行时
身份核验和控制面功能/清理复验，见[本轮报告](acceptance_controlplane_1e862c11_20260928_cn.md)。
集群已恢复 `50b9938f…` 基线；最新只读查询确认 generation/observed=140/140、
Ready=3。此前基线诊断仍不能归给 `8aa38da6…` 候选。

## 2026-09-28 TLS 环境恢复结果

- 已使用原 CA 重签 peer/client/probe/info 四份过期叶证书，保留原密钥、
  CN、SAN、EKU、KeyUsage 和 BasicConstraints；四份新证书的 CA 验证通过。
  沿用原 7 天有效期，新叶证书到期时间为 **10 月 5 日 04:56:19–20 UTC**。
  下次运行前必须重新检查有效期，不能把本次状态永久视作健康。
- 只通过 UID/resourceVersion/旧 data 前置条件更新对应四个 Secret 的
  `tls.crt`。Secret UID、CA、密钥及其他 data 字段均未改变；私有备份不入仓库。
- 当前运行版本支持握手时重载证书，实际没有重启 Pod。三个 KubeBrain 成员
  均已恢复 Ready，restartCount 仍为 0，StatefulSet generation 仍为 138。
- 从现有实例执行只读 TLS 检查：三个 Pod 的 client/peer/info 共 9 个端口均
  通过 CA 链、服务名及新叶证书 SHA-256 指纹校验。未使用跳过 TLS 校验选项。
- 前后对比确认命名空间内原 Pod UID/containerID/imageID/restartCount/运行
  状态、StatefulSet/PVC 身份及完整 spec 未变。没有部署候选、创建测试 Pod、
  操作磁盘或切换事务协议。此结果只证明环境恢复，不证明 RPC 或产品验收通过。
- 私有证据目录：`/root/.local/state/kubebrain/tls-renewal-20260928.uVWX9dgi/`。
  含前后快照、Secret 私有备份、叶证书、TLS 校验日志和已验证的 `SHA256SUMS`；
  不要将整个目录上传仓库。历史签发目录中的过期证书未覆盖，后续计划需引用
  新证书并重新核验凭据，不能复用旧计划摘要。

## 首版必验清单提案

以下六类用例已获用户确认，不表示已通过或已经确认了所有未量化细节。
所有正式结果须绑定最终候选源码/镜像；先恢复健康基线，再选择并核验候选。

| 项目 | 复用入口与环境 | 负载/门限与通过条件 |
| --- | --- | --- |
| 数据语义与鉴权 | `pkg/server/etcd` 官方客户端用例（内存后端）；`hack/etcd-client-compat` 的差分用例需连接真实 TiKV/PD 支撑的候选服务；`hack/backend-integration/run-real-local.sh` 另验证底层存储协议和选主 | 固定测试集合及 etcd 对标版本；KV/Txn/revision/Watch/Lease/Compact/Auth 与告警保护断言全部通过，无已知阻断性差异。存储协议普通/race 结果不能替代服务端 RPC 的真实后端验证；差分入口的 TLS/鉴权连接方式尚需与候选部署核对 |
| 真实 Kubernetes 接入 | `hack/scale-lab/controlplane-smoke.sh`、`controlplane-reference-smoke.sh`，隔离控制面分别连接候选与参考 etcd | 复用现有 Deployment/ReplicaSet/调度流程；开启 KWOK 时仅模拟节点/Pod 状态，不宣称真实容器执行；固定版本、规模与清理门限后执行 |
| 持久化与前端故障转移 | `hack/production/run-kubebrain-rollout-availability.sh` 的既有重启/硬故障模式；专用 TopoLVM 后端 | 沿用 6000 次、100ms 间隔、适用的 900s 完成上限、公共 5s/直连 30s、PD TSO/TiKV Region 各 1s；数据及 Watch/Lease 检查通过，恢复清理通过。不等同于宿主机断电 |
| 过期租约精确竞态 | 既有 `lease-term-probe` 和原生故障执行路径；真实 TiKV/PD | 保留原 30s 故障门限；实际命中过期撤销阻塞叠加任期丢失场景并验证原请求结果。完整入口仍有缺口，仅补运行该场景所必需的连接，不继续做通用框架 |
| 备份恢复 | 既有 rollout 探针中的 Snapshot、官方 etcdutl Restore 和恢复后官方 etcd 验证 | 实际恢复并核对当前/历史 KV、Watch 历史、Lease 和 Auth，不能只下载快照。TiKV 原生备份恢复的具体场景与恢复目标另列待确认，不以 etcd 导出恢复冒充 |
| 升级与回滚 | 既有 rollout 入口及本地盘回滚配置；固定基线与候选 | 持续负载覆盖升级与回滚，保留原 6000/900s 等对应门限；恢复完整配置、固定镜像及默认 2PC，独立检查资源清理 |

历史参考：[本地盘硬故障报告](acceptance_local_2pc_hard_failover_bb89c3f8_20260916_cn.md)、
[持续负载回滚报告](acceptance_local_rollback_df783b7f_20260917_cn.md)、
[控制面入口说明](../hack/scale-lab/README.md)。旧报告是配置/用例线索，不是新候选通过证据。

## 需确认的边界

- “完全兼容”是否继续遵循已有数据面兼容性矩阵，允许明确披露的维护/成员
  管理平台替代？若要求所有 etcd RPC 无差异，现有边界不满足，不能自行缩义。
- 长期 soak 的时长、跨故障域/后端故障组合、生产规模、TiKV 原生备份恢复及
  RPO/RTO 尚需明确是否属于本次必验项和具体标准。保留为待确认，不默认为
  排除，也不为追求“生产就绪”擅自加入无限测试。

## 2026-09-28 控制面清理条件诊断

真实 Kubernetes 接入的历史失败包含一个清理条件冲突，需要先确认处理方式，
不能通过改变产品的 Lease 语义来消除失败。

- 现有 `hack/scale-lab/controlplane-backend.sh` 删除专属前缀后，要求全局
  LeaseList 在 60 秒内自然清空，不执行 Revoke。`controlplane-smoke.sh`
  仅在 KubeBrain 模式调用此检查；reference 模式的 cleanup=0 不能证明
  参考 etcd 满足相同条件。
- 使用已核验来源的 `/root/etcd/bin/etcd`，对标提交
  `5cd9f4ee13801e18825d661e5005ae599460bc3a`，在全新私有数据目录和
  loopback 23579/23580 启动单成员诊断实例。授予 3660 秒租约、附着一个键、
  删除专属前缀并确认无键；期间不 KeepAlive、不 Revoke。
- 删除后 **61 秒**，LeaseList 仍有该租约，GrantedTTL=3660、剩余 TTL=3598、
  attached keys=[]。这直接证明参考 etcd 不满足上述长租约清理条件；不是
  KubeBrain 同候选真实后端验收，也不是完整 Kubernetes 流程的参考重跑。
- etcd `server/storage/mvcc/kvstore_txn.go` 删除键调用 Detach；
  `server/lease/lessor.go` 的 Detach 只解除附着、不删除租约。仓库现有
  `TestClientDoOpDeleteLeasedPointKeyWithPrevKVMatchesEtcd` 也断言删掉最后
  一个键后租约仍有效。本轮未重跑该单元测试，未修改产品或测试门限。
- [历史控制面报告](acceptance_controlplane_replacement_742e_ab043_20260917_cn.md)
  记录相同 GrantedTTL=3660、空附着租约导致清理失败。Kubernetes 源码中的
  默认一小时 Event TTL 加租约复用余量可解释该数值，但本轮没有追溯证明
  那两条历史租约的具体创建请求；不将这一解释当作已证实的全部故障原因。
- 原始脚本、二进制哈希、诊断日志及 etcd 日志保留于
  `/root/.local/state/kubebrain/reference-lease-cleanup.gZ4sjpRg/`。
  诊断脚本退出 0；结束时主动 TERM 有界 timeout supervisor，其 wait 返回
  124，如实保留，不作为产品请求超时。etcd 日志确认关闭，两个监听端口
  均已释放，数据目录留作证据；未操作测试集群。

**2026-09-28 用户已确认**：将控制面功能判定与租约清理分开；停止写入并删除
专属前缀后，验证前缀为空、租约无附着键，再按实际 GrantedTTL 单独核验
自然到期，完整清理未完成前不声称整体通过、不启动要求零租约的新测试。
不缩短 Kubernetes Event TTL、不主动撤销来源不明的租约。该调整只涉及
控制面长租约清理条件，原 30 秒故障门限及滚动负载门限不变；历史失败不改判。

## 对应产品回归与入口覆盖核验

2026-09-28，在本地源码 `a97789ad2fb7b5dc7ec53560114e29eb5287b3d8`
运行以下现有测试，未改产品代码或测试断言：

```sh
GOFLAGS='' GOWORK=off GOTOOLCHAIN=go1.26.8 go test ./pkg/server/etcd \
  -run '^(TestClientDoOpDeleteLeasedPointKeyWithPrevKVMatchesEtcd|TestDetachedEmptyLeaseExpiresNaturallyWithoutPublicRevision)$' \
  -count=1 -v -timeout=120s
```

两项均 PASS（整包测试执行 4.120s）：删最后一个键后租约仍有效且可重新
附着；点删除/前缀删除后的空租约自然过期，持久记录不复活，也不额外推进
公开 revision。夹具使用 `memkv`，这里的持久记录断言不是磁盘持久性证明。
本次为普通单测，不是 race、CI、真实 TiKV/PD 或新候选镜像验收。
原始日志：`/root/.local/state/kubebrain/lease-detach-regression.CZcrGEEB/unit.log`。

入口核对：`run-real-local.sh` 仅编译执行 `pkg/storage/tikv` 和
`pkg/backend/election`，不运行上述服务端 Lease 测试。现有
`hack/etcd-client-compat/lease_differential_test.go` 可作差分入口线索，但其
客户端构造未传 TLS/鉴权配置，不能直接把当前 mTLS 集群地址填入后声称已具备
可运行条件。本轮未运行该差分入口，也未降低集群 TLS 防护来迁就测试。

## 真实 TiKV/PD 路径的租约诊断

2026-09-28，使用根模块固定的官方 client/v3 v3.7.1，对当前固定基线
`sha256:50b9938fe3e5ad379136957438abe3c2d4bf250dc6c5973c869e0ef1341d1537`
执行了一次小范围诊断，不是新候选正式验收，也不是此前 3660 秒场景的完整重跑。

- 目标为专用命名空间中的 `kubebrain-local-0`，Pod UID
  `99667e6c-524d-4cc5-bb31-39d8c2166013`；配置指向独立 `kb-local-pd`
  后端、`kubebrain-local` keyspace，默认 2PC。Status 返回 cluster ID
  `7686251028133611667`、member ID `3358157933`。
- 本机 loopback 端口转发，仅创建独立键 `/diagnostic/lease-detach/nFI6JXpH`
  和本次返回的租约。使用原 CA、新 probe 证书及服务 DNS 名完整校验 mTLS，
  不改 hosts、不关闭服务端或客户端证书校验、不修改集群网络策略。
- 首次连接按 `127.0.0.1` 端点校验，因缺少 IP SAN 被拒绝，Status 未成功、
  尚未 Grant/Put；原失败日志保留。第二次使用服务 DNS 端点，底层拨号映射
  到本机转发端口，证书校验通过后执行诊断。这是连接配置修正，不是产品重试
  或验收门限调整。
- GrantedTTL=15；删键后 TTL=14、附着键=0、revision=44117。随后未发送
  KeepAlive/Revoke，约 14.995 秒后 TTL=-1、LeaseList 不再含本次租约、
  键仍不存在，最终 revision 仍为 44117。诊断及转发清理退出 0。
- 后置核验：23379 监听已释放；StatefulSet UID 不变、generation/observed
  138/138、Ready=3、固定镜像不变；目标 Pod UID 不变、restartCount=0。
  没有全局删键或清租约，也没有切换候选或注入故障。
- 私有证据目录 `/root/.local/state/kubebrain/real-lease-detach.nFI6JXpH/`：
  `main.go`、首次 `result.log`、修正连接后的 `result-2.log` 和两次转发日志。
  成功日志 SHA-256：
  `e9b0f476cd296e25584fe951205b7d7b132cbfdc0054da17fa8f041660e99a1a`。
  诊断程序只引用私有凭据路径，不包含凭据内容；没有新增仓库测试框架。

该结果补齐当前基线上“删键不撤销租约、空租约自然到期”的真实后端证据。
它不证明长期租约精确到期、故障恢复或新候选兼容性全部通过；控制面清理条件
调整已获用户确认，历史失败保持原判定。

## 已确认规则的入口修复与验证

2026-09-28，用户回复“同意这两项，继续”后，修改现有
`hack/scale-lab/controlplane-backend.sh`，不新增通用执行框架：清理先删除并
核对专属前缀，采样空租约的最大 GrantedTTL，加原 60 秒清理余量作为固定
观察预算，后续轮询不延长。每轮检查后端身份，遇到新增租约、残留附着键、
无效 TTL、RPC 失败或超时则失败；不会 Revoke 或 KeepAlive。
`controlplane-smoke.sh` 在清理前保存 `operation-result.json`（cleanup=pending），
完整清理后才写最终 `result.json`。原功能、故障和性能门限没有改变。

- `TestControlPlaneBackendOwnership` 普通回归：18 个场景通过，13.479s，
  包含超过原 60 秒仍在 GrantedTTL 窗口内、真正超时及窗口外迟到到期。
- `go test . -run '^TestControlPlane' -count=1 -race -timeout=120s`：通过，
  15.086s，覆盖现有控制面准入、清理、替换、RBAC 和审计测试。
- 修改后的清理函数已在固定参考 etcd 上实际运行：授予 5 秒租约并附着键，
  helper 删除专属前缀后观察到自然到期、零租约，诊断退出 0；etcd 和两个
  loopback 监听已停止。此运行验证真实响应解析与函数接入；长 TTL 时间分支
  由加速测试覆盖，本轮不是一小时完整控制面验收。
- Shell 语法和 `git diff --check` 通过。原始参考运行脚本、日志与 race 日志：
  `/root/.local/state/kubebrain/controlplane-cleanup-reference.PZivkoXX/`。
  实际运行 helper SHA-256 为
  `7558679710c094220e2a378571769a355a5e85fef6807c39003710e9822add66`。

上述入口修复验证阶段没有部署候选。随后候选上的控制面及清理复验已完成，
详见独立报告；本地入口变更尚未获新 CI 覆盖，不能将单个用例通过写成首版全部通过。

## 紧接着执行的工作

当前固定候选复验的进展与证据见
[控制面复验报告](acceptance_controlplane_1e862c11_20260928_cn.md)。

控制面用例及恢复清理已通过，下一优先项为原 30 秒租约故障用例。
2026-09-28 重新核查并执行当前代码的退任定向 race 回归：
`go test -race -count=1 -timeout=3m ./pkg/server -run '^(TestPeerRetirement|TestRetirement)' -v`
通过，64.608s；现场选举参数用例记录 partition→helper release callback 为
26.033s，原始过期续租流和凭据重载后的网络交接用例均通过。真实网络使用
内存存储故障注入，不是 TiKV 分区或正式 30 秒验收。原始日志：
`/root/.local/state/kubebrain/retirement-candidate-1e862c11.hJibfWa3/regression.log`。

执行入口的当前缺口已由源码核实：`lease-fault-plan` 仅预检；
`RunNativeCommand` 要求具体在线准入回调，当前无生产命令调用它；PID-1
清理/Join 尚未接入完整执行命令。采栈分类器也尚未登记 `1e862c11`，虽然
该提交 `lease.go` 摘要与已登记等待点版本相同。不得用空回调绕过准入，
不得仅增加分类器条目或打包工具就声称完成。后续只补齐运行这一具体用例
必需的连接并实际运行；不复用旧驱动、过期证书或已消费收据。
不再因历史证书故障扩建通用测试框架，也不把环境维护成功充当验收结果。
