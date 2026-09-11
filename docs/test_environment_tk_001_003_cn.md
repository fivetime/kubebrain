# tk-001-003 测试环境交接记录

最后核验：2026-09-11。本文件记录用户明确授权的测试环境，供长会话恢复时重新核验；不以历史状态代替实时检查。

## 最新分段延迟观测通过（非滚动升级）

2026-09-11，CI `34561935083` 和独立审计通过，探针源码 `2ef9d266`；镜像索引
`sha256:8a6899e6e3eb3c02011abd8e344f16ac38a246d5084efd7f6e999a7449dbce87`，
实际 Pod amd64 镜像摘要 `sha256:7f985abb925922d15d63efff4fe54b0bdddb044bdb51ee93d1fc507261a91ccb`。
探针 `kb-e2e-split-cprg7d8r`，UID `c93a444a-b12c-440c-8c71-6b7ebab98300`。
仍为 OBSERVE_ONLY、300 次、100 ms、public/direct 5s/30s、完成窗口 900s；
服务端保持 `0ce85e66`，无滚动或协议切换，命令退出 0。

循环最后一条 `completed=300 final=false`：elapsed=89955 ms，backend=2068 ms，
public=54455 ms，其中 put_resolve=46776 ms、watch_after_put=7678 ms，
direct_wait=3259 ms、pacing=30162 ms。阶段毫秒取整存在差异；final=true 的
91643 ms 另含循环后检查，不能计入单次请求耗时。平均写入确认155.92 ms、
随后公共 Watch 等待25.59 ms，不等同纯服务端耗时。
public watch=300、direct=300×3，Range=70、Snapshot=1，无 stream 重试；
lease 公共51/直连141次响应，无重启。最大 public1491/put1457/watch_after150/
direct2289/TSO3/Region415 ms。前次偶发认证错误未复现，但仍未解决。

清理报告 keys/users/roles/leases=0；独立确认测试 Pod、清理 Pod 和 owner ConfigMap
均不存在。StatefulSet UID/spec/generation22/Ready3/revision855b5bfb88 未变；
三个服务 Pod 的 UID/containerID/imageID/restartCount/Ready 均未变。
前缀 `/kubebrain-rollout-availability/kb-e2e-split-cprg7d8r/` 已用并清理，不复用。
证据：`/root/.local/state/kubebrain/auth-diagnostic-observe.cPrG7d8R/`。
这不是原 6000 次滚动升级验收；不能与其他负载的 TiKV 测量直接相减推导开销。

## 前次分段延迟观测失败（非滚动升级）

2026-09-11，CI `34557288219` 成功，源码 `29038dabc382070e367587b160d9f9667c60ab4a`。
独立校验确认镜像索引、双架构、实际 amd64 版本/客户端依赖及新增探针字段；
探针镜像为 `ghcr.io/fivetime/kubebrain@sha256:28e7d0dcf3183aa62a33b98190e19b45a4475b8d6cf4d50513d7a32725ae4a2b`。
临时提取的二进制和审计容器已删除。服务端维持 `0ce85e66`，无滚动或协议切换。

沿用 300 次、100 ms、public/direct 5s/30s、完成窗口 900s。
探针 `kb-e2e-split-u8lzxe0v`，UID `5b13094f-8afc-43de-a0b0-e86a87a6e552`。
命令退出 1：第 209 次迭代发现后台 Snapshot integrity probe 失败，官方恢复集群的
exact-reader 授权 Watch 返回 `Unauthenticated: etcdserver: invalid auth token`。
最终 `completed=208 total=300 elapsed_ms=61673`，不是通过结果；最后一条进度可能
包含失败迭代的部分计时，不能当作完整 300 次样本均值。根因尚待定位。

最终 cleanup 报告 keys/users/roles/leases=0；独立检查无探针/清理 Pod 或 owner
ConfigMap。StatefulSet UID/spec 未变、generation=22、Ready=3、revision
`kubebrain-855b5bfb88`；三个服务 Pod 的 UID/containerID/imageID/restartCount/Ready
均与测试前一致。前缀 `/kubebrain-rollout-availability/kb-e2e-split-u8lzxe0v/` 已用并清理，不复用。
原始日志、镜像校验回执及前后身份：`/root/.local/state/kubebrain/e2e-split-observe.u8lzxE0V/`。
本机旧 TLS 恢复测试重复 10 次通过（265.139 秒）；新增默认 bcrypt 成本、
`CN=root` 客户端证书组合也重复 10 次通过（272.038 秒）。两组均未复现，不能
据此关闭故障；调查证据在
`/root/.local/state/kubebrain/restored-auth-investigation.8peghe7t/`。

## 前次通过的基线端到端观测（非滚动升级）

2026-09-11，规范 `run-kubebrain-rollout-availability.sh` 以 `OBSERVE_ONLY=true`、
300 次操作、100 ms 间隔运行，public/direct 门限保持 5s/30s、完成窗口保持 900s。
未设置 TARGET_IMAGE，不滚动、不切换协议。探针与服务均为现有不可变镜像 `0ce85e66`。
Pod `kb-e2e-observe-9rw2jxvi`，UID `7d60ad37-7f0f-4971-8cff-4557c73e1b3a`。
命令退出 0；公共 watch=300、三个直连 watch 各 300，lease 存活（公共 48、直连 132
次响应，无重启），Range=70、Snapshot=1，无 stream 重试。

最后一条计数循环内进度（`final=false completed=300`）如下：

| 阶段 | 累计耗时 | 每次平均 |
| --- | --- | --- |
| PD/TiKV 健康检查等前置工作 | 1.829 s | 6.10 ms |
| 公共写入开始至公共 watch 接收 | 51.783 s | 172.61 ms |
| 公共 watch 之后额外等待直连结果 | 0.621 s | 2.07 ms |
| 固定间隔（含调度偏差） | 30.164 s | 100.55 ms |

循环总计 84.405 s；上述阶段之外约 8 ms 为其他工作和取整差异。随后 `final=true`
记录为 86.042 s，包含循环后的检查，不应把多出的 1.637 s 归入单次写入延迟。
public 最大延迟 962 ms、Put 确认最大 941 ms、Put 后 watch 最大等待 257 ms；
这些最大值不能替代各阶段均值。主要非固定等待在公共路径，但当前镜像未细分其均值。
下一版已加入 `put_resolve_ms`、`watch_after_put_ms`，本次实际结果不能回填新字段。

canonical cleanup 确认 keys/users/roles/leases=0。独立检查确认探针、清理 Pod 和 owner
ConfigMap 均不存在；StatefulSet UID、spec、generation=22、revision=`kubebrain-855b5bfb88`、
Ready=3 前后完全一致，三个服务 Pod 的 UID/containerID/imageID/restartCount/Ready 也一致。
已用前缀 `/kubebrain-rollout-availability/kb-e2e-observe-9rw2jxvi/` 不复用。
完整日志、前后身份、私有环境包装脚本及摘要：
`/root/.local/state/kubebrain/e2e-observe.9rW2jxvi/`。没有额外保留编译二进制。
这是当前基线短时诊断，不代表原 6000 次升级通过；不能直接与 `cab503a8` 后端内部
固定 256 字节工作负载做减法来推断认证/代理开销。

## 最新受控后端延迟测量

2026-09-11，非特权 Pod `kb-latency-dnifb5m6`，UID
`6bd55648-6f37-4a31-8356-d2f6c4ae3a3c`，节点 `k8s3-compute1`，restart=0。
上传前后二进制 SHA256 一致：
`ef9dbf49bf2c27f217e6d063466f917c881e159aa785a1ecc4aa64df5a6267d8`。
同一构建分四个独立进程执行固定后端更新测量（2PC/1PC/1PC/2PC），均 PASS；
平均耗时分别 46.85/28.31/30.50/45.00 ms。请求协议计数、配额、watch 和清理检查通过。
同构建另外复验不确定结果已提交/未提交分支，两次 PASS。均非公共 RPC 或升级验收。
以下 nonce 对应的 `kubebrain/protocol-smoke/<nonce>/` 前缀及
`protocol-backend-<nonce>` keyspace 已用并清理，禁止复用：

- 四组测量：`07c04e2cfd443c648c395dbf2f69ecb2`、`c9c223569b232934bd8caa633b5aec5e`、
  `5bbcac3f13a60f0c4021124d7fed167a`、`aa4125ca3360569f6e81c0604e94de42`。
- 两分支复验：`47b24de7f0240eecdf42131f64b47f3e`、`8ac99d83d4081001c21d3ee5346ddb09`。

每次均由后端 Close 后的独立客户端完成所有权 CAS 清理并检查双范围为空。
Pod 已按 UID 前置条件删除并独立确认不存在，本机辅助二进制经摘要核验后删除。
未更新现有服务或生产协议；核验 StatefulSet gen/observed=22/22、Ready=3、
revision=`kubebrain-855b5bfb88`。原始样本与结果见
`/root/.local/state/kubebrain/backend-latency.dNiFb5M6/results.json`，同目录保留日志、
构建和清理证据。小样本后端均值不能替代 900 秒升级门禁。

## 本机升级探针取证修复（尚未重新执行真实升级）

2026-09-11，检查最近失败升级的执行日志与脚本，发现完成窗口超时未触发回滚后
探针日志收集。已修复 `run-kubebrain-rollout-availability.sh`：完成超时设置取证标记，
EXIT 仍先回滚，再核验探针 UID，限时/限量收集日志，最后进行原有所有权清理。
日志不可用、超大或挂起时也必须继续清理。没有修改集群或重新运行升级；原失败结论、
900 秒/6000 次/100 ms 间隔及 public/direct 门限均不变。
后续真实执行应保存整个命令输出；回滚后收集的最后进度不等于截止时刻完成数。
本地复现和回归证据目录：`/root/.local/state/kubebrain/rollout-timeout-evidence.VggBfAIa/`。

## 最新协议故障验证：真实后端未提交解析及已提交复验

2026-09-11，非特权 Pod `kb-backend-absent-ejf6ofnf`
（UID `62b09d96-8e3d-4f37-909d-6d9232f7987d`）分别用独立进程精确运行
`TestRealTiKVBackendResolvesUndeliveredOnePC` 和
`TestRealTiKVBackendResolvesCancelledOnePC`，两次 PASS。
上传前后二进制 SHA256 一致：
`91b1568679ca3540708ac6d9bb49d8db5777ee79a1e8e46eefd90d02eb589df2`。
未送达分支解析 absent=1、committed=0，候选 101 不推进可见修订号，下一次 CREATE
复用 101；已提交复验解析 committed=1、absent=0，双键 101、下一次 PUT 为 102。
两次均通过后端 Close 及所有权 CAS 清理，两个派生范围分别确认为空。
以下前缀及同 nonce 的 `protocol-backend-<nonce>` keyspace 已用并清理，不复用：

- 未送达：`kubebrain/protocol-smoke/e2a575cb685594c94f3c1ef46318bda3/`
- 已提交：`kubebrain/protocol-smoke/3507a804144a487df11736ca946a2a66/`

Pod 已按 UID 前置条件删除并独立确认不存在，本机二进制核验摘要后删除。
未更改网络、PD/TiKV、StatefulSet 或生产提交协议；核验 gen/observed=22/22、Ready=3、
revision=`kubebrain-855b5bfb88`。日志、构建和清理证据保留于
`/root/.local/state/kubebrain/real-backend-absent.Ejf6ofNf/`。

## 前次协议故障验证：真实后端持久见证解析

2026-09-11，非特权 Pod `kb-backend-onepc-52dx4lzj`
（UID `651f8721-9288-4a05-bb01-f1765ce6e261`）精确运行
`TestRealTiKVBackendResolvesCancelledOnePC`，PASS。上传前后 SHA256 一致：
`8805277002a5c8644db7ae557b3d3b016a2ace0730c0b36dbed2dc2edf4e2248`。
不确定双键事务被真实后端持久见证解析为已提交，双键事件/读取为修订号 101，
下一次单键写入/事件为 102。未改集群网络、PD/TiKV、StatefulSet 或生产提交协议。
测试后端不运行选主、全局 GC 或自动压缩；本次不覆盖这些路径或进程重启。
后端 Close 和独立客户端所有权清理均通过，两个隔离范围确认为空。
已用并清理的前缀 `kubebrain/protocol-smoke/91d6c43172c62399cc4914b4d3afd624/`
及 keyspace `protocol-backend-91d6c43172c62399cc4914b4d3afd624` 不复用。
Pod 已按 UID 前置条件删除并独立确认不存在；本机二进制核验摘要后删除。
StatefulSet 核验 gen/observed=22/22、Ready=3、revision=`kubebrain-855b5bfb88`。
日志、构建信息、源码摘要和清理记录：
`/root/.local/state/kubebrain/real-backend-onepc.52dX4Lzj/`。

## 前次协议故障验证：成功响应丢失后取消调用者

2026-09-11，独立非特权 Pod `kb-onepc-cancel-5ztqwumt`
（UID `12b1bd6b-ca86-4c49-81ee-eefb770d0b68`）精确运行
`TestRealTiKVOnePCCancelAfterResponseLoss`，PASS。二进制上传前后 SHA256 一致：
`107cfdb5556a50a07d551c5550e16d65a25e848cb37615c499c8447df4973758`。
实际服务端提交后丢弃响应并取消提交子上下文；适配器返回不确定结果，只有一次尝试。
独立上下文读取、更新和历史快照检查通过，所有权清理确认前缀为空。
已用并清理的前缀 `kubebrain/protocol-smoke/d5381498cb716c3a535b8562b96b4aad/` 不复用。
Pod 已按 UID 前置条件删除并独立核验不存在；本机辅助二进制核验摘要后删除。
日志、源码摘要及构建信息保留在 `/root/.local/state/kubebrain/real-onepc-cancel.5ZtqwUmT/`。
未更新 StatefulSet，核验 generation/observed=22/22、Ready=3、revision=`kubebrain-855b5bfb88`。
生产 1PC 未启用。此项仍不是 KubeBrain 后端持久见证解析验证。

## 前次协议故障验证：成功响应丢失后默认重试通过

2026-09-11，在独立非特权 Pod `kb-onepc-loss-4m8d48mk`
（UID `d1d568cb-6548-4522-9049-0cdbcc329436`）中运行精确存储测试
`TestRealTiKVOnePCResponseLoss`。上传二进制摘要与本机一致；只包装此测试客户端，
收到真实 1PC 成功响应后丢弃一次，不改集群网络、PD/TiKV 或现有服务。
默认重试实际发送两次并返回成功，两键可见性及历史快照检查通过，清理确认前缀为空。
已用并清理的前缀为 `kubebrain/protocol-smoke/ff65b9ff7f86dc4dcfb3d5c68e16184e/`，不复用。
Pod 已按 UID 前置条件删除并独立确认不存在，本机辅助二进制经摘要核验后删除。
源码、构建信息、清理及实际分支记录位于
`/root/.local/state/kubebrain/real-onepc-response-loss.4M8d48MK/`。
本次不是不确定结果返回分支，不代表 KubeBrain 后端见证解析或 Raft 故障持久性通过；
生产 1PC 未启用，严格滚动升级仍未通过。

## 最新存储协议 smoke：独立 2PC / 1PC 通过

2026-09-11 使用独立非特权 Pod `kb-protocol-smoke-mccb2vmu`，
UID `092aa50e-2e87-47fe-a79f-1c34b7672721`，无 API 令牌或宿主机挂载；
上传的静态测试二进制摘要与本机一致。连接 `kb-pd.kubebrain-dbaas-test.svc:2379`，
预期及实际 PD 集群 ID 均为 `7683177044639569228`，分别运行精确存储协议 smoke。
2PC / 1PC 实际成功计数、固定时间戳历史值与当前两键读值均通过，各自清理确认前缀为空。
已用并清理的前缀（不复用）：

- `kubebrain/protocol-smoke/3cc8888ca014140253924e8ef9244f80/`（2PC）
- `kubebrain/protocol-smoke/98bccf5e16053290b33a164f2fb64946/`（1PC）

Pod 已按 UID 前置条件删除并独立确认不存在；本机测试二进制在摘要核验后删除，
源码、摘要、构建信息和日志保留于 `/root/.local/state/kubebrain/real-protocol-smoke.MccB2Vmu/`。
生产服务未改，仍为下述 generation 22/22、Ready 3 的基线；未重启 PD/TiKV。
这不是后端见证解析、Raft 故障持久性或性能对比，也不代表严格升级验收通过。
完整操作约束见 [真实 TiKV 协议 smoke](tikv_protocol_smoke_cn.md)。

## 最新升级：旧对象读取去重候选超时，基线已恢复

2026-09-11，产品 `ff5dc1a8`、发布 `5fe3d007` 的本地提交前后完整检查、后端 CI
`34542018818`、镜像 CI `34542018828`、独立镜像核验及只读预检均通过。
候选 `sha256:34569ba7d4f33395ea75828184f0ac04db7772729fac7abf8dbb0e663aa6fb59`
完成三个副本滚动更新，但探针未在滚动完成后原 900 秒内完成 6000 次操作，执行 exit 1。
公共 5 秒、直连 30 秒、0.1 秒间隔不变；本轮仍未通过严格升级验收。

自动回滚及独立复核完成：StatefulSet UID 不变，generation/observed 22/22、Ready 3，
current/update 均为 `kubebrain-855b5bfb88`，三个 Pod 实际镜像均恢复为
`sha256:0ce85e66320b27bf4cdb8f11981a76cba58926fa0e835fc47dc663f709b588ce`，重启计数均为 0。
新 Pod UID：0=`8898a115-ceb1-41ed-ae2d-4119ecf70b53`，
1=`f8463861-a6d9-416c-afac-8b776cf4f34f`，2=`29dcf24d-7551-43af-862d-8af6467fd6cc`。
`prepull-upgrade-5fe3d007` 及镜像 holder 已删除，测试键/用户/角色/租约均无残留，
日志包含 `PREPULL_CLEANUP_CONFIRMED`。继续使用消费者 `rook-ceph` 存储；未重启 PD/TiKV。
证据：`/root/.local/state/kubebrain/tk-001-003/txn-previous-5fe3d007.HS3eG0ol/`，
包含实际 CI/镜像记录、执行日志、回滚后对象快照及同进程指标增量。
该候选已正式失败，不应在没有新证据或修正的情况下重复执行。下文为历史状态。

## 上一轮升级：事务配额批读候选超时，基线已恢复

2026-09-10，产品 `31c2954f`、发布 `9a0fdd3b`，后端 CI `34522265523` 与镜像
CI `34522265522` 成功；独立镜像核验和只读集群预检通过。仍使用消费者集群
`rook-ceph` 的 `nvme-rep3-rbd-pool`，未使用 secondary 集群，未重启 PD/TiKV。
候选镜像为 `sha256:edeb87ad2353c4f724449e905e2784bee3a2f9dfab95913855c6b3df3f4a1d60`。
原 6000 次 / 0.1 秒间隔 / 公共 5 秒 / 直连 30 秒参数不变；三个候选副本完成滚动后，
探针未在原 900 秒完成期限内结束，执行 exit 1，不算验收通过。

自动回滚后 StatefulSet UID 不变，generation/observed 为 20/20，Ready 3，
current/update revision 均为 `kubebrain-855b5bfb88`。三个 Pod 实际运行的基线镜像均为
`sha256:0ce85e66320b27bf4cdb8f11981a76cba58926fa0e835fc47dc663f709b588ce`，重启计数均为 0。
新 Pod UID：0=`dae210aa-516e-4281-9475-172ad8e2bedc`，
1=`22f30964-c0a6-4d5e-9f4b-d70cb85076c7`，2=`b3566cfa-2078-4273-bb8d-6ce67d7c5376`。
本轮 `prepull-upgrade-9a0fdd3b` 探针及 `kb-prepull-60b35ac985d43caeec1be106c6658d57-`
前缀的 holder Pod 已无残留，日志确认测试数据无残留及 `PREPULL_CLEANUP_CONFIRMED`。
证据目录：`/root/.local/state/kubebrain/tk-001-003/txn-quota-9a0fdd3b.ewMYJKjv/`，
含 `execute.log`、镜像核验记录、回滚后对象快照、同进程指标增量。
该候选已经正式失败，不应在没有新证据或修正的情况下重复执行。
下文旧 generation 18 及旧 Pod UID 是历史记录，不代表当前状态。

## 最新消费者接入：独立 apiserver 基础 smoke 通过

2026-09-10，执行仓库内 `hack/dev/apiserver-smoke.sh`（源码 `eddc122c`，脚本摘要
`ebd1f11a1aa92acd7263f090264db7f4307a11ffa79738508eb32b8521de1abc`），
通过独立 Kubernetes v1.36.1 apiserver 连接本测试集群的真实 KubeBrain/TiKV。
未滚动升级，实际服务仍为修复版镜像 `sha256:0ce85e66320b27bf4cdb8f11981a76cba58926fa0e835fc47dc663f709b588ce`，
StatefulSet UID `2650ad15-1d37-41c4-836c-d40dd4502720`、generation/observed 18/18、Ready 3。

使用显式 kubeconfig/context `kubebrain-test-10.32.32.66`、namespace `kubebrain-dbaas-test`，
客户端 Service `kubebrain-client` UID `a0fbf77b-010b-4595-a28b-471b73d2d640`。
仅将该 Service 的 3379 转发到本机 loopback 13379，TLS 验证未跳过，使用已有私有客户端证书；
连接确认 cluster ID `7683177044639569228`，前置 revision 29346，前端租约数为 0。
测试专属前缀 `/registry-kubebrain-apiserver-tk001003-20260910-eddc122c` 预检为空，
独立 apiserver 仅监听 loopback 18443，没有复用默认 kind 节点名或改变本机默认 kubeconfig。

apiserver 来自已有 `kubebrain-dbaas-control-plane`，kind 镜像摘要
`sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5`。
提取的可执行文件实际版本 v1.36.1，SHA256
`9b4dba0a5b945f1fe0ce18f47535c5ff0c46ae384f9222047bce39fe91b6023e`。
etcdctl 来源经现有脚本核验为 `/root/etcd` 提交 `5cd9f4ee13801e18825d661e5005ae599460bc3a`。

执行 exit 0：ConfigMap update/watch 返回 `update ok`/`watch ok`，标签选择器 5、复合选择器 2，
字段选择器命中 batch-3，chunk-size=2 分页合计 5，批量删除后为 0；Secret、Lease 更新、
零副本 Deployment 创建/更新均完成。独立 apiserver 没有工作节点或调度控制器，不创建业务容器。
脚本会回收基线后新增的前端租约，因此本次与其他租约写入/升级探针串行，禁止并行复用此入口。

结束后另行只读核验前缀为空、租约数仍为 0；临时 work/PKI 目录与 apiserver 进程已移除，
本次端口转发已停止，13379/18443 均无监听。已删除此次提取的二进制及空提取锁/目录，
可从原 kind 镜像重新提取；预先存在的私有 TLS 材料未删除，也未加入 Git。
完整日志/来源/清理结果位于私有目录 `/root/.local/state/kubebrain/consumer-smoke.BVGOKrII/`。
该目录保留证据，不是第二份测试代码权威；运行入口仍以仓库脚本为准。
本结果只证明基本消费者接入，不证明 KWOK 规模、长时间 watch、故障恢复或 30s/900s 升级达标。

## 最新升级验收终态：c7d9905e 配额批读候选

发布源码 `c7d9905e57526c4d2868f96805b7e276f37888c0`（产品提交 `260d51e1`）
CI 34470793561 于 2026-09-10 11:50:24 UTC 成功。独立核验发布索引
`sha256:19627458c114cd0780351718a7964fa06fbfc6cc0996efa2f7ca1b7a92fb576e`、
amd64 实际二进制版本/构建信息及非 root 配置；客户端仍为固定 fork `832b70fd622f`。
候选曾达到 generation 17、Ready/updated 3/3，但正式升级探针未在滚动完成后
900 秒内完成 6000 次操作，执行会话 2580 已终止 exit 1。public 5s、direct 30s、
每次操作后 0.1s 间隔未改变；没有完整 PROBE_SUMMARY，正式验收仍失败。

随后自动回滚，实时 API 确认 generation/observed 18/18、Ready/updated 3/3、
current/update revision `kubebrain-855b5bfb88`；三 Pod spec 镜像和实际 imageID 均为
`ghcr.io/fivetime/kubebrain@sha256:0ce85e66320b27bf4cdb8f11981a76cba58926fa0e835fc47dc663f709b588ce`。
Pod0/1/2 UID 分别为 `690f9743-45db-473a-a7f6-15e24b448e61`、
`8eb96328-6169-4a29-af34-5a51bbcf61ad`、`e581a277-bb50-43fc-a70b-facf209daf79`，
全部 Ready、无删除标记、重启数 0。日志确认最终 fixture keys/users/roles/leases 全零、
PREPULL_CLEANUP_CONFIRMED；API 列表确认探针 `prepull-upgrade-c7d9905e` 及三个
`kb-prepull-1ab2779498d5f90340c48f0fd1537462-` holder 均不存在。

候选同进程两次指标采样间，603 次 Put 平均 87.17ms，959 次批提交平均 45.77ms；
store 2001 的 Commit / Prewrite 均值分别约 20.09 / 18.19ms，导出 backoff 计数为零。
采样顺序执行而非原子快照；不同层级样本数不同、耗时重叠，不可直接相加。
上一轮涉及 store 2004，不是受控 A/B；不能宣称优化有效或退化，亦不能认定物理存储根因。
旧修复版 Pod2 同 UID 状态确认 exit 0 Completed；有界观察器自然结束，
watch_exit=124、parse_exit=0、capture_exit=0，日志跟随和两次指标采样均已结束。

证据位于仓库外 `/root/.local/state/kubebrain/tk-001-003/`：
`security-prepull-c7d9905e-execute.log`、`security-prepull-c7d9905e-image-verified.json`、
`security-prepull-c7d9905e-metrics-delta.md`，执行目录
`prepull-c7d9905e-execute.Ylq761AN8m3y`，预拉取 journal `prepull.8zVnX0NUkTEH/attempt`。
不要重新轮询旧会话或将历史运行中记录视为当前状态。后续继续定位性能及隔离消费者回归，
不重复盲跑、不放宽门限、不自动启用 1PC/async commit。以下环境阶段记录需按时间区分。

## 授权与连接

- 用户授权通过 SSH `root@10.32.32.66` 登录控制节点部署测试，并允许复制远端 `/etc/kubernetes/admin.conf` 到本机。
- 控制节点 hostname：`control1.cloud.local`；Kubernetes Node：`control1`。
- 本次首次连接接受并记录的 ED25519 host key 指纹：`SHA256:9oKH+KtMEDDPFHpzwscXj1OzLnRTw9uzbtGMNfhoU7c`；后续变化应停止并核验，不自动覆盖 known_hosts。
- 本机 kubeconfig：`/root/.kube/kubebrain-test-10.32.32.66.conf`，权限 `0600`，位于仓库外。
- 本机该文件中的 context：`kubebrain-test-10.32.32.66`；cluster：`tk-001-003`。
- 集群身份锚点：`kube-system` namespace UID `ca72e2c8-53a6-4c40-ba04-6d07b2b03aa6`。
- 每条 kubectl/Helm 命令显式指定该 kubeconfig；不切换默认 kind context，不操作原 `kind-kubebrain-dbaas` 实验数据。
- SSH 密码由用户通过会话提供，不记录在本文、Git、命令行参数或日志；kubeconfig 的私钥、证书及 token 内容同样不得提交或打印。

## 存储硬边界

**仅使用 rook-ceph 消费者集群的 StorageClass；禁止使用 rook-ceph-secondary 集群。**

已核验并选定的 StorageClass：`nvme-rep3-rbd-pool`。

| 字段 | 核验值 |
| --- | --- |
| StorageClass UID | `d3283670-e235-4676-ad5a-11f42ce6526e` |
| provisioner | `rook-ceph.rbd.csi.ceph.com` |
| parameters.clusterID | `rook-ceph` |
| pool | `nvme-rep3-rbd-pool` |
| provisioner/node-stage/controller-expand Secret namespace | 全部为 `rook-ceph` |
| filesystem | `ext4` |
| reclaimPolicy / binding / expansion | `Delete` / `Immediate` / `true` |

所有本次 PVC 必须显式指定该 StorageClass，不能依赖默认 `nvme-ec42-rbd-pool`。创建后还需核对 PV 的 CSI driver、volumeHandle、claimRef 以及实际文件系统容量。不能修改现有 StorageClass、Ceph pool 或 rook-ceph-secondary 的任何对象来使测试通过。`rook-ceph/rook-ceph-external` 显示 external.enable=true、health=HEALTH_OK，但 phase=Progressing；是否可供卷必须以实际 PVC/挂载测试确认。

## 已观察的环境与部署边界

- Kubernetes `v1.36.0`，Ubuntu 26.04、amd64，CRI-O `1.35.3`；11 个节点当前 Ready。
- 候选测试 worker：`k8s3-worker1/2/3`，IP `10.32.32.70/71/72`；每节点 allocatable CPU=15、内存约 28 GiB，无 taint。
- 三 worker 没有 zone 标签；独立 Node 不等于已证明独立物理宿主机或可用区，不伪造 zone 标签。
- worker 上存在用户既有 Cilium、OpenStack、rook-ceph-secondary 等工作负载。**本次部署授权不等于可重启整台 worker、隔离宿主网络或影响既有 Ceph 服务；此类故障注入需另行确认精确范围。**
- 本次新工作负载限定于独立 namespace `kubebrain-dbaas-test`，UID `6c57c242-912b-41bb-9020-f4fdb3225ef3`；首次核验该 namespace 不存在。共享 CRD/Operator 等前置资源先检查已有安装和兼容性，不覆盖用户现有配置。
- 首次检查没有 TiDB Operator/TidbCluster CRD；本次已新增官方 v1.6.5 CRD 和 namespace-scoped Operator。Prometheus Operator 仍未安装。
- VolumeSnapshot 三类 CRD 原已存在。按 Deployment 名称检查未发现独立 snapshot-controller，但进一步按容器镜像核验，在 `openebs/openebs-zfs-localpv-controller` 中发现通用 `snapshot-controller:v8.2.0`；已复用它，未安装第二套控制器。rook-ceph CSI 的 snapshotter sidecar 为 v8.5.0。
- 部署阶段使用小数据量与有界测试资源；还原/故障测试只针对本次创建并核验 UID 的专属对象，不删除用户已有 PVC/PV。

## 进度

### 已部署的独立后端

- Helm release：`kubebrain-tidb-operator`，namespace 为 `kubebrain-dbaas-test`，chart/app 均为 v1.6.5；`clusterScoped=false`、`scheduler.create=false`、controllerManager 两副本。共享 nodes/PV/StorageClass 权限仍按官方 chart 配置，不能把 namespace-scoped 描述成完全没有 cluster-scope RBAC。
- CRD 来自 [官方 v1.6.5 清单](https://raw.githubusercontent.com/pingcap/tidb-operator/v1.6.5/manifests/crd.yaml)，下载文件 SHA-256 为 `a90106c487335a942e4ade08a757cc07484d9c26101a04d00352bdabb4409281`；创建前已确认没有 PingCAP CRD。
- TidbCluster `kb`，UID `28d3e529-6790-4dd1-a6c9-7bccc48a11f1`；PD/TiKV v8.5.3，3+3 副本，不部署 TiDB SQL。
- PD requests 为每副本 1 CPU/2 GiB/20 GiB PVC，TiKV 为 4 CPU/8 GiB/100 GiB PVC；limits 分别为 2 CPU/4 GiB 和 8 CPU/16 GiB。仅限 worker1/2/3，每组件硬 hostname 反亲和。
- 六份数据 PVC 全部为 `nvme-rep3-rbd-pool`，独立 PV UID 与 CSI volumeHandle，clusterID 全部为 `rook-ceph`；后端共申请 360 GiB 逻辑容量。`pvReclaimPolicy=Retain`，初始 `maxFailoverCount=0` 控制资源增长；这不是生产故障接管配置验收。
- 最终 `kb-pd-0/1/2` 分别位于 worker3/1/2；`kb-tikv-0/1/2` 分别位于 worker1/2/3；六 Pod Ready、restart 0。PD cluster ID `7683177044639569228`，三个 store ID `2001/2004/2005` 为 Up。
- 已观察的 PD imageID：`docker.io/pingcap/pd@sha256:b32c69d8b9cc08cead83649d54c58942c441492b459c4cf190cd8c4747bf85f3`；TiKV imageID：`docker.io/pingcap/tikv@sha256:00502c3c74915577ff3a669a223a3740c3f1f7e151de745620a95a5f1450a909`。当前 CR 使用版本 tag，尚未宣称完成镜像供应链发布门禁。

首次配置有两个已处理的问题：instance label `kubebrain-test` 与期望的 `kb` 不一致，且 NetworkPolicy 误选 discovery，阻断其读取 API。已收紧 policy 只选择 PD/TiKV，并统一 instance label 为 `kb`。由于 selector 不可变，按本次对象的 UID/resourceVersion 删除重建 discovery/PD 工作负载，保留原三份 PD PVC；未删除用户已有资源。首次 PD 启动出现过一次重启，最终重建后的六个数据 Pod 均为 restart 0，不能用最终计数抹掉该启动历史。

### rook-ceph 快照恢复验证

- 创建了专属 VolumeSnapshotClass `kubebrain-test-rook-ceph-rbd`，driver 为 `rook-ceph.rbd.csi.ceph.com`，clusterID 和 snapshotter Secret namespace 均为 `rook-ceph`；不是默认 class。
- 源 PVC `storage-smoke`：UID `ed73d09a-c7a1-421e-bd21-c6c8a679a2af`，1 GiB，在 worker2 挂载；实际文件系统容量 996,780 KiB，不是宿主根盘容量。
- 源文件 65,536 bytes，sync 后 SHA-256 为 `84f879be8ef136113274a48472fe2953005de77fd112759a745ff93d297d59f8`。
- VolumeSnapshot `storage-smoke`：UID `ab1f45ed-cb7a-4df5-a6e0-c2d30cbf8baf`，readyToUse=true；content UID `6e348164-e040-4d8e-901f-abf49b84d434`，source volumeHandle 精确绑定源卷。
- 恢复 PVC `storage-smoke-restored`：UID `724bb1fa-ef78-4817-b81d-b5d048437e86`，仍显式指定同一 rook-ceph StorageClass，但分配不同 volumeHandle；在 worker3 挂载后逐字节摘要相同，验证 Pod Succeeded。
- 该结果证明此 CSI 路径的基本快照/跨节点恢复，不代替 KubeBrain native PITR、多个数据卷的一致性恢复或跨可用区故障证明。两个 1 GiB 测试卷及快照暂时保留供后续核验。

### Region/存储门禁与网络边界

原样运行 `validate-tikv-region-health.sh` 的 API Service proxy 路径返回 `cannot read PD stores response`。Cilium 定向 drop 观测确认 proxy 源为 `240.16.0.57`、identity `remote-node`；既不是外部 CIDR 身份，也不是 `kube-apiserver` 实体，且集群 `enable-node-selector-labels=false`。没有更改 CNI 全局配置或放开所有 node。两份无效诊断放行 policy 已按 UID/resourceVersion 删除；保留 namespace 内 PD/TiKV peer 通信和 DNS 的隔离策略。

使用本机适配器 `kubectl-pd-exec.sh`，仅将该门禁的六类只读 PD GET 改为固定 `kb-pd-0` 容器内 localhost curl，其余 kubectl 请求原样执行；每次请求先核对 Pod UID `a06f3224-daac-4966-a69a-033c0fbe8d4b`，Pod 替换后必须重新审计并更新锚点。门禁判定代码、PVC/PV/claimRef/容量/水位阈值和采样次数均未修改。结果：3 PD、3 Up store、连续 3 次无异常 Region、六卷身份/容量/磁盘水位通过。**这是 exec 传输适配后的通过，不是原 Service proxy 入口已恢复。**

### 本机持久材料与下一步

非凭据部署材料位于 `/root/.local/state/kubebrain/tk-001-003/`（目录 0700），包括 namespace/PVC/探针、快照/恢复、TidbCluster 清单、Operator 渲染输出与只读传输适配器。`storage-gate-pd-exec.log` 保存门禁终态输出。TidbCluster 清单 SHA-256 为 `818538d9b94e26dba221a958c58d42a96b9256fe752638c2bb1780aa116250f2`，适配器 SHA-256 为 `0b8b8817ab9c0adebfe3d8df0ce872239d4718fab56595d05edb58119f989bd5`。

KubeBrain 本体尚未部署：已有本机 A5788 OCI 包，但新集群无已发现的镜像 registry Service，控制节点到 worker 的 BatchMode SSH 认证失败。未向其他节点复用控制节点密码，也未修改 worker 的 CRI-O/证书/registry 配置。用户随后确认使用 KubeBrain 仓库的 GitHub Actions 发布镜像，要求 `runs-on: self-hosted`，并授权推送 `dbaas`、触发构建和使用已登录的本机 `gh` 跟踪状态。

### CI 镜像交接约定

- 仓库为 `fivetime/kubebrain`，不是 kubetron；不将 `main` 合并进当前开发分支，不强推。
- `dbaas` 分支的 `.github/workflows/image.yml` 使用 self-hosted Runner，支持该分支 push 与手动触发；构建仓库根 `Dockerfile` 的 TiKV 镜像，平台为 linux/amd64 和 linux/arm64。
- 先推送 `ghcr.io/fivetime/kubebrain:dbaas-<完整 commit SHA>`，核对双架构索引、运行时版本/SHA/构建时间、TiKV 后端、OCI labels 与非 root 用户后，才更新 `:dbaas`；不改 `:latest`。
- 部署时使用成功运行返回的 `ghcr.io/fivetime/kubebrain@sha256:<digest>`，不能仅凭可变 tag 或 workflow 已触发就认定镜像可用。需记录 Actions run URL、源 commit、最终 digest、验证结果，并在测试集群核对实际 imageID。
- 本节记录配置与授权；构建成功、worker 拉取能力和 KubeBrain 部署仍须以运行结果核验，不以计划代替成功记录。

本次已快进推送 `8963d2ff590bb4a0b4f779f701843c34a9c5a350` 到 `dbaas`，由 push 自动触发 [Actions run 34247183115](https://github.com/fivetime/kubebrain/actions/runs/34247183115)，未重复手动派发。self-hosted Runner `raas-1482` 构建、双架构索引验证、运行时 metadata 验证及 `:dbaas` 标签发布均成功；镜像为 `ghcr.io/fivetime/kubebrain@sha256:033668da577610611b5e9c82b81f7002d062320135463043a940d4f7cc4c7b81`，未覆盖 `latest`。该镜像对应客户端分仓前的产品提交，不能记为分仓后的构建结果；测试集群尚未部署该 KubeBrain 镜像。该提交前后均执行 `--verify 4` 与四个生产测试分片，两轮各 703 项全部通过；workflow 的 Go 回归测试及 actionlint v1.7.12 也通过。

### 客户端分仓后的真实传输测试

独立客户端固定版本、补丁历史、CI 和安全扫描结果见[分仓维护说明](tikv_client_maintenance_cn.md)。本次使用远端依赖编译原有 `TestLargeKeyRoundTripTiKV`，通过 worker3 上专属 Pod `client-split-large-key` 连接 `kb-pd`；PD cluster ID 与本环境锚点一致。600 KiB 和 2 MiB 物理 Key 的 Put/Get/Iter 校验通过（1.95 秒），Pod UID `0291d0d9-6b06-4de1-a035-f346c433577f`、Succeeded、exit 0。用例清理自己的前缀；未新建 PVC，不接触 rook-ceph-secondary。

二进制 SHA-256 `a1af25f6b947367e974806ecc5a465e45504ab3fca9775e8f19684a0e7491058`；结果保存在上述仓库外材料目录的 `client-split-large-key.log` 与 `client-split-large-key-result.json`。测试 Pod 按 UID 删除，本机临时二进制已清理。该测试不是 KubeBrain 本体部署，也不是整个产品恢复/故障验收。

分仓镜像 [run 34253661949](https://github.com/fivetime/kubebrain/actions/runs/34253661949) 已 success，
源 commit `522da3334662200606d01e32dad8556238a3fc40`，双架构 digest
`sha256:1e1dd21b06c7cd0ff1fbfda04ea26d55f14e98949ca9333c0eff4f8fd12871cb`。
该镜像仍是 Go 1.26.5 的旧安全基线，尚未部署；不得用其构建成功代替后续安全修复镜像的验证。

后续安全升级客户端 `b5b63af11282` 的真实大 key 复测也已通过，1.45 秒；专属 Pod
UID `743095a9-d977-41f5-954a-896f906ed101`，已按 UID 删除，本机临时测试二进制已清理。
环境材料目录新增 `client-security-large-key.log` / `client-security-large-key-result.json`，
详情和未完成门禁见[安全升级记录](security_baseline_20260908_cn.md)。

补充安全核验：镜像原附带官方 kubectl v1.36.2 的二进制扫描报告 18 项漏洞；现采用独立模块
重编译的 `v1.36.4+kubebrain`，保留 v1.35/v1.36 server 的版本差窗口。Go 1.26.8 下的
amd64/arm64 二进制扫描均无漏洞报告，容器构建输出与本地扫描产物逐字节一致。使用该 amd64
工具只读核验本环境 namespace UID、PD cluster ID 和 server-side dry-run，未创建实际 Pod。
API `/version` 本次返回 v1.36.1；此前 v1.36.0 来自节点 kubelet 信息，二者不混同。
本机全局 kubectl 和用户 Kubernetes 集群未升级。记录位于环境材料目录 `security-kubectl-*`；
临时下载的候选工具和本地/容器构建输出已清理，后续可按固定模块及 metadata 重新构建。

安全更新产品提交 `5645caa6b014c2c17b84af968f242efeee3a388a` 已完成提交前、提交后的
703 项生产四分片验证并推送。新镜像由 push 自动触发
[run 34264830428](https://github.com/fivetime/kubebrain/actions/runs/34264830428)，首次回读 queued；
待验证成功后记录不可变 digest。完整本地预提交镜像的 66 个 Go 可执行文件均通过二进制扫描，
该验证镜像、提取容器和临时二进制已清理；`security-product-image-*` 日志留存。
本地构建使用预提交 metadata，不冒充该产品提交的发布产物。

取得新安全基线已验证的镜像后，部署三副本 KubeBrain、客户端语义测试及产品恢复验证，再补监控和故障演练。不要将当前后端与 CSI 冒烟测试标为整个产品验收完成。

### 安全基线部署准备（2026-09-08）

镜像 run `34264830428` 已由 self-hosted Runner `raas-1494` 接手，源码模块扫描和 kubectl
双架构二进制扫描均 success，产品镜像构建仍 in_progress。文档提交 `d1f63ee8` 使用
`[skip ci]`，未取消或替换该代码提交的构建任务。

本次重新核验两个 namespace UID、StorageClass UID/rook-ceph 参数、PD cluster ID；
原判定脚本通过同一只读 exec 适配器再次得到 3 PD/3 Up store、连续 3 次无异常 Region、
六份后端数据卷身份/容量/磁盘水位通过。日志 `security-predeploy-storage-gate.log`。

基于产品 `deploy/production/kubebrain-tls.yaml` 准备的测试 overlay 已通过 server-side dry-run：
三副本限定 worker1/2/3，保留硬 hostname 反亲和、非 root、只读根文件系统和 mTLS；
独立 keyspace/cluster-name 为 `kubebrain-dbaas-test`。本次小数据测试设逻辑配额 2 GiB，
每副本 snapshot/spill 各 4 GiB、spill 进程上限 2 GiB，共将新增 24 GiB 逻辑工作卷。
全部显式使用 `nvme-rep3-rbd-pool`；独立 PVC 不等于独立物理 Ceph I/O 故障域，
不以本测试规格证明生产容量或性能。尚未创建这些 KubeBrain 工作卷或 StatefulSet。

已创建的前置对象：

- PriorityClass `kubebrain-dbaas-critical`，UID `2b5d406a-0ae0-4e38-bcfc-9f34e7c18037`，
  value=1000000、非默认、`preemptionPolicy=Never`；未修改既有 PD/TiKV 工作负载或其他用户 Pod。
- namespace 内 NetworkPolicy `kubebrain-test-isolation`，仅选择本次 KubeBrain 标签；
  允许同 namespace 通信及 kube-dns，未放开外部 namespace 或节点网络。
- Secret `kubebrain-client-tls` / `kubebrain-peer-tls` / `kubebrain-info-tls`，UID 分别为
  `6209f0a8-36e1-4abe-96f7-f26fe1aebdc9`、`17124849-aa91-4033-81f2-a41e5730bcea`、
  `155aa30d-d10b-4778-9c53-da2716b3b3ee`。
- 单独的探针 Secret `kubebrain-test-client-tls`，UID
  `d9ed215c-0818-413e-88d4-04e41d4122e3`，仅包含客户端 CA 和 CN=root 的测试客户端叶身份，
  不向探针提供服务端或 CA 私钥。

三套独立测试 CA 与叶证书、独立 CN=root 的测试客户端身份存于材料目录的 `tls/`，
目录 0700、私钥 0600，不提交 Git。客户端叶证书到期时间为 2026-12-07 18:51:12 UTC；
测试 PKI 不等于生产证书签发/自动轮换已验收。当前 PD/TiKV 仍为隔离网络内明文，
前端/peer mTLS 不替代后端传输加密，后续必须单独完成该门禁。

材料目录新增 `generate-test-tls.sh`（拒绝覆盖已有 tls 目录）、`kubebrain-test-overlay.sed`、
`kubebrain-network-policy.yaml`、`kubebrain-tls-candidate.yaml`。candidate 仍含 `kubebrain:dev`
占位，仅作 dry-run，**不可直接部署**；取得成功 CI 的固定 digest 后重新渲染并审计。
继续禁止对整个材料目录执行 apply，避免应用历史诊断或未完成清单。

待执行的 `kubebrain-security-smoke-template.yaml` 同样必须先替换 IMAGE_DIGEST/STATEFULSET_UID；
使用镜像内既有 audit/availability 二进制，限定 worker3、非 root、无 SA token、900 秒总寿命。
计划逐副本运行事务读写审计，再完成 300 轮公共/三副本直连 Watch、Lease、RangeStream、Snapshot
与 PD TSO/TiKV Region 探针。其独占前缀为
`/kubebrain-rollout-availability/security-smoke-5645caa6/`，外部预记录 lease IDs 为
`2026090819050001/2026090819050002/2026090819050003`，程序会拒绝复用已存在 ID。
该探针尚未创建、测试尚未运行，不把计划或模板当作 PASS。失败后必须先按 Pod/StatefulSet UID
和上述 fixture 身份核验清理，不能直接重跑来掩盖残留；成功后也需回读数据面零重启、卷身份与工作目录清理。

### 发布产物核验中（后续架构检查已拒绝此候选）

固定源码标签现已上传，双架构 digest 为
`sha256:6ab33dc81572dfc318b02d4f840252111790690e1fa04d20af1f772db309a224`。
独立下载 amd64 后，版本/SHA/Go 1.26.8/TiKV/构建时间/OCI labels/非 root 均匹配；
实际镜像中的 66 个 Go 可执行文件二进制扫描全部通过。证据与边界见
[安全基线记录](security_baseline_20260908_cn.md)。CI 同一 run 仍 build/push in_progress，
未宣称发布完成或部署成功；继续等待 Verify/Promote 及终态。

从固定源码 `5645caa6` 加测试 overlay 重新渲染 `kubebrain-tls-5645caa6.yaml`，
全部镜像使用上述 digest，文件 SHA-256 为
`5d8d39df467d8f660d10095a87e1b790531d2fc0cc26cb437a7d7b7928ca07f8`。
再次通过 server-side dry-run，且逐项核对恰好五个 namespaced 对象、三副本、两类工作卷的
rook-ceph StorageClass 与 4 GiB 配置；没有创建 StatefulSet 或这些工作卷。

本次读取实时负载时，Metrics API 返回不可用；没有安装或修改用户的 metrics-server。
节点资源请求和 Ready/Pressure 状态可读，调度余量不替代实时 CPU/内存负载或监控系统验收。
PD/TiKV 六个 Pod 的 UID 未变化，均 Ready、restart 0。

后续核验直接使用 arm64 子镜像 digest，发现其主程序与 kubectl 实际均为 x86-64，
与 amd64 产物字节完全相同。因此 **`6ab33dc8…` 不可部署**，此前索引/amd64 安全扫描通过
不等于全平台发布通过。错误来自 Dockerfile 的 TARGETARCH 默认值覆盖自动平台参数，
修复和失败/通过证据见[安全基线记录](security_baseline_20260908_cn.md)。
`kubebrain-tls-5645caa6.yaml` 清单已被判定为无效候选，不能因为此前 dry-run 通过而创建它；
StatefulSet 和六份 KubeBrain 工作卷仍未创建。保留原有 TLS/隔离前置资源，等待修复提交的新镜像。

原 run `34264830428` 已在 Verify published test image 步骤失败，Promote skipped；未部署候选。
修复已通过真实 BuildKit 自动平台回归和完整 arm64 编译阶段的 69 个程序架构/漏洞扫描；
其中 66 个属于主运行镜像，三个属于独立备份/恢复镜像。提交前 703 项四分片也已全部通过。
这些本地验证尚不能代替修复提交的新 CI 和真实部署。错误候选及编译阶段的本地镜像、大体积
提取文件均已清理，日志保留；最后暂留的 kubectl 及整个
`/tmp/kubebrain-release-audit.rIYu3W` 临时目录也已删除，未替换全局 kubectl 或清空共享缓存。

架构修复已本地提交 `19c23ca6`，提交后 verifier 及全部四分片通过，尚未推送。
CI 原失败日志显示先遇到同一索引 digest 切换平台拉取的 `cannot overwrite digest`，并非已经
运行到 arm64 `cmp`。下一步还须改成按平台子镜像 digest 拉取，再对修复后的整套工作流
重新提交验证；旧镜像的 arm64 程序错误已由本地直接子镜像提取独立证明。
不能跳过该拉取错误或部署 amd64-only 候选来宣称双架构发布完成。

`19c23ca6` 提交后测试结束，已开始后续工作流修复：验证阶段按唯一子镜像 digest
pull/run/create/inspect，提升阶段继续使用完整双架构索引。解析器与工作流回归已加入，
完整提交前后测试及新镜像 CI 尚待完成。当前仍不得部署旧候选。

子镜像 digest 修复的提交前 verifier/四分片现已全部通过（703 项）；提交后完整验证与
新镜像 CI 仍是后续门禁。详细日志名、时长与验证范围见[安全基线记录](security_baseline_20260908_cn.md)。

2026-09-08 20:11 UTC 只读复核：kube-system 与测试 namespace 的 UID 仍匹配前述锚点；
PD/TiKV 六个 Pod 均 Ready、restart 0；`kubebrain` StatefulSet 查询仍为空。
`nvme-rep3-rbd-pool` UID、CSI provisioner、clusterID 及三项 Secret namespace 仍匹配
rook-ceph 消费者集群。未新增 KubeBrain Pod/PVC 或修改集群配置；后续实际部署前仍须重新核验。

### 后端 TLS 门禁预审（未执行迁移）

当前前端/peer/info 证书不覆盖 PD/TiKV。KubeBrain 已提供 `--tikv-ca-file`、
`--tikv-cert-file`、`--tikv-key-file` 和 `--tikv-verify-cn`，但本测试 overlay 尚未挂载
后端客户端证书或配置这些参数，不能把前端 mTLS 当作端到端加密验收。

已查阅 [TiDB Operator v1.6 组件间 TLS 指南](https://docs.pingcap.com/tidb-in-kubernetes/stable/enable-tls-between-components/)：
现有明文集群的迁移不等价于普通滚动修改 `spec.tlsCluster.enabled`，官方流程涉及 PD
临时缩容、TLS 重启、内嵌 etcd peerURL 变更和恢复副本。须先审计维护窗口、现有数据保护
与回退步骤，不能直接在当前三副本上试改开关，也不删除既有 PVC 来绕过迁移。
证书应使用独立后端 CA，按 Operator 约定提供 `kb-pd-cluster-secret`、
`kb-tikv-cluster-secret`、`kb-cluster-client-secret`；SAN 必须覆盖实际 peer/service DNS。
迁移后需同步 HTTPS PD 探针、客户端 TLS、只读适配器的新 Pod UID 锚点，以及正向连接、
错误 CA/CN 拒绝、Region 健康和大 Key 回归。本文只记录预审，不代表这些资源或步骤已执行。

预审运行 `go test ./cmd/option ./pkg/storage/tikv -run 'Test.*(TLS|Security|CN)' -count=1 -v`：
cmd/option 的前端 TLS 参数、后端证书全有或全无校验、后端参数绑定共三个顶层测试通过；
pkg/storage/tikv 明确输出 `[no tests to run]`，不计为后端传输测试通过。
证据为材料目录 `security-backend-tls-unit-preflight.log`。本轮按 VerifyCN/ClusterVerifyCN
检索仓库测试，仅定位到参数绑定断言；须补充实际后端 TLSConfig 的可信 CA/匹配 CN 正向
握手与错误 CA/CN 拒绝回归，并与授权 TiKV/PD 真正开启 TLS 后的端到端测试分别记录。

### 修复镜像 CI 已触发

子镜像 digest 修复 `339381afb74eb225d7bab8e67196ccea07596509` 的提交前后两轮 verifier
及四分片（各 703 项）均已通过，前置架构修复 `19c23ca6` 也已完成自己的两轮验证。
两提交已快进推送 dbaas，自动触发 [run 34275611099](https://github.com/fivetime/kubebrain/actions/runs/34275611099)，
创建于 `2026-09-08T20:34:39Z`，首次状态 queued，源码 SHA 一致；尚未取得可部署的固定
digest。继续保留旧候选 `6ab33dc8…` 的拒绝结论，不以新 CI 启动代替发布门禁通过。

### 后端 TLS loopback 回归已补充

`pkg/storage/tikv/security_test.go` 新增实际 TLS 1.2/1.3 双向握手与客户端证书轮换测试，
覆盖及限定范围见[客户端维护说明](tikv_client_maintenance_cn.md)。新用例普通测试与 race
连续 10 次通过；整个存储包本机单测/race、vet 通过，但三个真实 TiKV 集成测试明确跳过，
不计为本环境后端 TLS 验收。日志为 `security-backend-tls-handshake.log`、
`security-backend-tls-handshake-race.log`、`security-backend-storage-unit.log`、
`security-backend-storage-unit-race.log`。首次用例误把 TLS 1.2 缺失客户端证书时的
handshake failure 断言成 TLS 1.3 的证书错误，后按协议分别断言，仍要求连接失败，未改产品逻辑。

负向日志 `security-backend-tls-isolated-baseline-negative.log`：在临时副本中只将固定
客户端的 `config/security.go` 替换为上游 v2.0.7 对应文件，wrong_CN 用例在两个协议版本
均因“期望错误但实际连接成功”失败；检查退出码及两个明确失败用例后才认定复现。
首次尝试直接 overlay 模块缓存被 Go 拒绝，日志 `security-backend-tls-upstream-baseline-negative.log`
不构成行为证据；当时外层命令未对后续匹配失败立即退出，打印的预期复现尾行无效。
后改用独立客户端归档、临时 modfile/overlay 并严格检查失败输出，未改正式 go.mod、fork
工作树或模块缓存。临时副本清理后只保留日志。新增测试尚未提交，不会改变在运行的
image run `34275611099`（源码仍是 `339381af`）。

临时归档 `/tmp/kubebrain-tls-baseline.L06FtU` 已完整删除；正式 `go mod verify` 全部通过，
fork 工作树保持干净，未清空共享 Go/BuildKit 缓存。新测试的提交前 verifier 确认 703 项
完整分配（170/193/180/160），四分片全部通过（251.971/452.596/308.033/731.406 秒），
日志为 `security-backend-tls-pre-verify.log` 及 `security-backend-tls-pre-shard-{0,1,2,3}.log`。
接着提交测试补充，并立即运行相同的提交后 verifier/四分片；提交后终态仍待回读。

镜像 CI `34275611099` 已由 self-hosted Runner `raas-1502` 接手，job
`102227752676` 开始于 `2026-09-08T20:35:09Z`。自动目标架构回归与 runtime manifest
选择回归均于 `20:40:06Z` 成功完成，build/push 从 `20:40:07Z` 开始，当前仍在运行。
这证明新增的两个构建前检查已在 Runner 执行，不替代后续实际镜像字节/架构核验或最终发布成功。

为避免当前镜像构建被 concurrency 策略取消，TLS 测试补充提交暂留本地，不立即推送。
该提交仅增加测试及证据文档，不改运行时代码或客户端依赖；在运行镜像的源码仍为 `339381af`。

TLS 回归提交为 `0f78c31a4885cae154fd5833dff24e24b5fd75fa`，提交后 verifier 与四分片
703 项全部通过（285.912/486.156/322.032/764.257 秒），日志为
`security-backend-tls-post-verify.log`、`security-backend-tls-post-shard-{0,1,2,3}.log`。
提交后存储包普通/race、vet 也通过（本机模式，三个真实 TiKV 集成用例仍未执行），
日志为 `security-backend-storage-post-unit.log`、`security-backend-storage-post-unit-race.log`。
该提交仍仅在本地，未推送以免取消在运行的镜像 CI。

等待期间创建隔离工作树 `/tmp/kubebrain-staticcheck.gUgTUr/repo`（detached 于同一
`0f78c31a`）处理既有 52 项 Staticcheck 诊断；基线日志
`security-staticcheck-isolated-before.log` 与此前诊断一致。草稿仅在此工作树，尚未提交或
合回主工作树；须验证后迁移，并执行新代码提交前后完整测试。迁移完成前不要清理该目录，
完成后定向移除临时 worktree，不清空共享缓存。

### Staticcheck 清理与传输回归（2026-09-08）

隔离草稿已通过根模块 Staticcheck/vet，以及 objectstore 子模块 Staticcheck。删除的是
无调用的旧辅助函数和未使用赋值；保留告警解除的 CAS 冲突失败、事务见证验证、领导权
就绪后的 epoch、Watch 首响应校验。两个故意传 nil context 的负向测试保留原输入及
断言，仅对对应行标注 SA1012 的原因；未全局屏蔽诊断或减弱门禁。

native-pitr-preflight 从弃用的 DialContext/WithBlock 迁移到 NewClient，显式保留
passthrough 地址解析、连接 Ready 屏障和调用方 context。新增本机 TCP/gRPC 测试覆盖
HTTP/2 尚未就绪时等待、deadline/cancel、空请求正常响应、意外 checkpoint 和 RPC 错误。
普通测试通过；与 TiKV 存储包一起 race 连续三轮通过。protobuf 测试改用 V2/protoadapt
桥接固定的 V1 kvproto 消息，保留所有字节所有权、错误响应、复用对象和分配阈值断言。

核心包普通测试全部通过：backend 50.654 秒、server/etcd 143.087 秒、endpoint 22.601 秒、
etcdproxy 1.776 秒、storage/tikv 0.206 秒。备份验证和发布探针等七个受影响包也全部通过，
其中 rollout-availability-probe 124.463 秒。告警/事务见证/领导权相关筛选 race 在 backend
和 server/etcd 分别 6.342/12.066 秒通过，不等同于这两个包的全量 race。
TiKV 存储包仍未设置真实 PD 地址，三个环境依赖测试跳过，不计作真实 TiKV 验收。

编解码 Scan/Batch 两项 fuzz 各 30 秒通过（86,711/76,851 次执行）；本机分配 benchmark
为 generated 6,159 allocs/op、owned-frame 4 allocs/op，原分配阈值测试通过。此数据仅说明
测试 API 迁移没有丢失相应检查，不代表生产负载性能验收。

证据均在仓库外材料目录，前缀 `security-staticcheck-`：`isolated-final.log`、
`isolated-with-transport.log`、`isolated-vet.log`、`objectstore.log`、`core-unit.log`、
`probes-unit.log`、`transport-unit.log`、`transport-codec-race.log`、`fence-race.log`、
`codec-fuzz-scan.log`、`codec-fuzz-batch.log`、`codec-benchmark.log`。

已用 apply_patch 迁回主工作树，并逐字节比较全部 Go diff 和新增测试，确认一致后定向
移除 `/tmp/kubebrain-staticcheck.gUgTUr/repo` 及空父目录；改动完整保留在主工作树，未清理
共享 Go/BuildKit 缓存。主工作树 Staticcheck 再次通过（`main-pre.log`）。提交前 verifier
通过，仍是 703 项、170/193/180/160；四分片已启动，结果待回读（`pre-verify.log`、
`pre-shard-{0,1,2,3}.log`）。本批改动尚未提交，提交后门禁尚未运行。

`2026-09-08T21:37Z` 回读镜像 run `34275611099` 仍为 build/push in_progress；尚未执行
Verify/Promote，不推送新代码取消它，不部署旧的已拒绝镜像。本轮没有对测试集群执行写入。

补充构建检查：`go build -tags tikv ./...` 和 `go build -tags badger ./...` 均通过，
只使用 Go 编译缓存，不生成仓库 bin 产物。额外 `staticcheck -tags badger ./...` 失败于
既有 `cmd/option/initial_cluster_test.go` 三处直接引用 TiKV 专用 `pdAddrs` 字段；该文件
在 HEAD 与本轮工作树相同，属于此前未覆盖的标签测试编译缺口。普通根模块 Staticcheck
PASS 不扩大为 Badger 标签测试 PASS。日志为 `security-staticcheck-build-tikv.log`、
`security-staticcheck-build-badger.log`、`security-staticcheck-badger.log`。后续须保留
成员身份校验断言并使 fixture 按存储配置初始化，不能直接给通用测试加标签来跳过 Badger。

后续只读复查：namespace、SC、六个 PD/TiKV Pod UID 均与锚点一致，六 Pod Ready/零重启；
KubeBrain StatefulSet 仍不存在。使用原脚本及固定 PD Pod UID 的 exec 适配器重新通过
3 PD/3 Up store、连续三次无异常 Region 和六卷身份/容量/水位检查，日志为
`security-staticcheck-predeploy-storage-gate.log`。未变更集群资源或网络策略。

Staticcheck 清理的提交前 verifier/四分片 703 项全部通过：258.613/470.765/308.194/745.028 秒，
日志为前述 `security-staticcheck-pre-*`。随后提交本批变更，并立即执行相同提交后门禁；
提交后结果待回读。Badger 标签测试编译缺口仍单列保留，不影响本批默认 TiKV 配置的结果边界。

### 修复镜像发布成功（339381af）

image run `34275611099` 已终态 success。Verify published test image 于
`2026-09-08T21:44:22Z` 成功，Promote 于 `21:44:29Z` 成功，之后收尾也成功。
源码为 `339381afb74eb225d7bab8e67196ccea07596509`，不是本地 TLS 测试/Staticcheck 后续提交。
registry 按完整源码 tag 回读的不可变索引为：
`ghcr.io/fivetime/kubebrain@sha256:a245c95fea36c387358d86e3808a9d29073a327028d5a4e3a80e4d272663e865`。
linux/amd64 子镜像 `sha256:b14371c47b78fc7d3eaf632296ebd6158374becf38d3fee6cb96b6f27d5222b1`，
linux/arm64 子镜像 `sha256:d7429739f9e35c99cafef595286c9ba4a74a5a04bd29672597b2fa417ff94686`。
CI 实际两架构各 66 个 Go 程序的 GOARCH/ELF、运行时平台/SHA、kubectl 字节对比等发布检查
均通过；arm64 运行使用 Runner QEMU，不声明原生 arm64 生产验收。

重新渲染 `kubebrain-tls-339381af.yaml`，SHA-256
`01a841e2e5536f317ff84ba95aa895755baa98b76c26b9efb6ff9c4bbb0a79ee`，固定使用上述索引。
server-side dry-run 通过，并断言恰好五个 namespaced 对象、三副本、非 root/只读根文件系统、
两类 4 GiB 工作卷都使用 rook-ceph 消费者 SC。证据 `security-339381af-dry-run.json`。
首次汇总误按 List 解析 kubectl 输出的五个连续 JSON 对象，jq 失败；改用 slurp 并对五项
完整断言后通过，没有忽略校验错误。本记录时尚未创建 StatefulSet，下一步才执行部署。

### 三副本首次部署与冷启动延迟

已创建上述五个对象，StatefulSet UID `2650ad15-1d37-41c4-836c-d40dd4502720`；
ServiceAccount UID `6389933b-c121-446b-81a1-b2399ce9f0a7`、PDB UID
`9680eb28-e0d2-47ab-91e1-172b01d24215`，client/peer Service UID 分别为
`a0fbf77b-010b-4595-a28b-471b73d2d640` / `6a2695bb-e303-4788-999b-9ae4e35f30f4`。
创建响应保存在 `security-339381af-created.json`。六个 4 GiB 工作 PVC 已 Bound，均为指定
rook-ceph 消费者 SC。Pod 0/1/2 分别落在 worker3/1/2，UID 为
`f4f7c830-5a2a-485d-bd43-b53f18c53bcb`、`af235510-0e4b-4e55-bbb5-ff51df089311`、
`fcec128c-edce-4689-9576-8fd85c820864`。三个 Pod imageID 均报告上述固定索引；Pod 内实际
version 核对得到 TiKV、linux/amd64、Go 1.26.8、源码 `339381af`、构建时间 `20:35:19Z`。

首次 10 分钟 rollout 等待超时，不能写成一次正常启动。leader kubebrain-2 在 checkpoint
阶段反复报 `decode durable revision watermark: invalid revision watermark length 0`；
followers 正确拒绝向 NOT_SERVING leader 转发。只读诊断确认当前持久 watermark 是 1，
但 Store safe timestamp 为 `468946512982310913`（17:36:20.798 UTC），远早于本次启动。
该历史快照的 GetAt 返回 not found、BatchGetAt 不含该键；代码直接对缺失 map 值解码，
把历史快照尚未包含 watermark 表现成损坏值。不能通过当作当前 revision 或跳过安全时间戳
来放行。后续需要区分缺失/损坏，并调查冷启动安全时间戳推进延迟。

诊断程序只读指定 keyspace 的 watermark、TSO 和 range safe timestamp，没有执行数据写入、
GC、锁清理或集群配置变更；临时上传程序已从 Pod snapshot 工作卷移除。
日志 `security-339381af-checkpoint-readonly.log`。其中 compact 范围首次诊断使用了未附加
keyspace 后缀的协调前缀，不能拿它证明实际 compact Region；object/durable 范围和当前
watermark 读取直接使用正确的 `kubebrain-dbaas-test` coder，不受该范围错误影响。

三个副本随后自然变为 Ready、零重启；再次 rollout 观察通过，日志
`security-339381af-rollout-after-delay.log`，首次超时保留在 `security-339381af-rollout.log`。
启动错误/恢复记录为 `security-339381af-startup-watermark.log`、`security-339381af-startup-recovery.log`。
TiKV 的 resolved-ts.enable=true、advance-ts-interval=20s；没有更改参数来掩盖延迟。

下一步专属 probe 清单 `kubebrain-security-smoke-339381af.yaml` 固定同一镜像与 StatefulSet UID。
Pod 名 `security-smoke-339381af`，fixture 前缀
`/kubebrain-rollout-availability/security-smoke-339381af/`，预留 lease IDs 为
`2026090822060001/2026090822060002/2026090822060003`；旧 5645caa6 probe 未执行，旧 ID 不复用。
先三副本逐一审计，再跑 300 轮可用性/Watch/Lease/RangeStream/Snapshot 与 PD/TiKV 探针。
此处为执行前身份登记，尚未取得 probe 结果。

Staticcheck 提交 `e156a9f7e79b2d5aa20214bc80d5ee2e73e68b28` 的提交后 verifier 与四分片
703 项全部通过（255.788/455.628/308.469/731.352 秒），日志 `security-staticcheck-post-*`。
前后两轮均完成，随后快进推送；新提交的镜像构建不替代当前已部署 `339381af` 的验收身份。

快进推送已成功，自动触发 image run
`34284304697`（https://github.com/fivetime/kubebrain/actions/runs/34284304697），源码精确为
`e156a9f7e79b2d5aa20214bc80d5ee2e73e68b28`，当前 build/push in_progress，不重复派发或推送
后续代码取消它。当前服务继续固定使用已经验证的 `339381af` 索引。

综合探针已创建，UID `c4bad481-abbc-48ee-b8fa-d90181c1e660`，创建响应
`security-339381af-smoke-created.json`，持续日志 `security-339381af-smoke.log`。三个直连
审计均通过：依次 Put/Delete revision 为 2/3、4/5、6/7，同一 PD/数据面 cluster ID，
三个 60 秒 audit lease 的回收验证通过。随后综合探针打印 PROBE_STARTED；当前 Pod Running，
尚无 PROBE_SUMMARY/终态，不能把审计通过当作 300 轮、Snapshot 或恢复已通过。

六个工作 PVC/PV 的精确 claimRef UID 一一匹配、六个 volumeHandle 唯一，CSI driver 与
clusterID 全为 rook-ceph 消费者；清单为 `security-339381af-work-pvcs.json` 与
`security-339381af-work-pvs.json`。三个副本的 snapshot/spill 挂载实际文件系统均
4,046,560 KiB、当时使用 1%，不是 node-root 容量。只读诊断源保存在仓库外
`checkpoint-readonly-diagnostic.go`；本机 `/tmp/kubebrain-checkpoint-diagnostic.2LlyPP`
及其编译产物、Pod 内上传文件已清理，源码与日志仍可恢复诊断过程。

### checkpoint 缺失 watermark 错误分类修复草稿

根据真实启动观察新增 `TestSerializableCheckpointWaitsForDurableWatermarkVisibility`。
旧代码在“安全快照缺失 watermark”用例中实际返回 InvalidMVCCMetadata，负向测试按预期
失败；空值但存在的键仍要求损坏错误。修复读取 map 的存在位，缺失时返回带 timestamp
的 ErrSerializableCheckpointUnavailable，继续 fail-closed；不发布 checkpoint，不用
当前 watermark 或假定 revision=1 替代历史快照，也不改变 TiKV safe timestamp。
测试还验证 marker 真正进入后续安全快照后恢复创建 checkpoint。

`security-checkpoint-missing-before.log` 记录复现；修复后 checkpoint 系列普通测试
0.275 秒通过，新用例 race 连续 20 次 2.109 秒通过，backend vet/Staticcheck 通过。
日志 `security-checkpoint-missing-after.log`、`security-checkpoint-missing-race.log`。
该草稿只解决缺失与损坏混淆，不宣称修复了 10m26s 的真实冷启动 safe-ts 延迟。
新批次 verifier 已通过 703 项、170/193/180/160，提交前四分片运行中，日志前缀
`security-checkpoint-missing-pre-`；草稿尚未提交或包含在运行镜像内。Badger 标签 fixture
兼容性缺口仍待后续处理。

### 综合探针失败终态与恢复 TLS 诊断

`security-smoke-339381af`（UID `c4bad481-abbc-48ee-b8fa-d90181c1e660`）于
22:18:32 UTC 终止，Pod Failed、exit 1。最终错误为
`stream integrity probe did not complete one RangeStream and Snapshot within 8m2s: range=461 snapshot=0: context deadline exceeded`。
没有 PROBE_SUMMARY；三个直连审计通过不等于综合验收通过。曾观察到 33,681,440 字节
Snapshot 文件与恢复目录，只能证明进入恢复阶段，不能证明恢复验证完成。日志及终态分别为
`security-339381af-smoke.log`、`security-339381af-smoke-latest.json`。

另建精确绑定原 probe UID、StatefulSet UID、fixture 前缀及三个 lease ID 的清理审计 Pod
`security-smoke-339381af-cleanup`（UID `dc45ba83-0bfc-4fd5-b496-5f9d8d94d9a7`）。
22:21:49 UTC Succeeded、exit 0，输出 `FIXTURE_CLEANUP_OK status=absent`，
keys/users/roles/leases 全为零；审计在任何删除操作前确认不存在残留，没有额外删除数据。
证据 `security-339381af-cleanup.log` 与 `security-339381af-cleanup-result.json`。
两个终态 Pod 暂留，未重跑失败测试、未重启后端或放宽门禁。

代码核查发现恢复探针把源连接的 client certificate/key 同时用作本地嵌入式 etcd 的
server certificate/key。实际挂载的 test-client.crt 是 CN=root、仅 clientAuth、无 SAN；
现有 TLS 恢复测试使用含 serverAuth/clientAuth 与 SAN 的 SelfCert，未覆盖该合法客户端身份。
下一步以 client-only 证书复现并分离本地恢复 PKI；不向 probe 挂载生产服务端或 CA 私钥，
不关闭 TLS 验证。尚不能把此代码问题已定位写成修复后综合验收通过。

checkpoint 修复提交前 verifier 与四分片已全部通过，703 项，耗时
253.171/451.373/305.544/728.379 秒，证据 `security-checkpoint-missing-pre-*`。
22:29:59 UTC 查询 image run `34284304697` 遇 GitHub API rate-limit 403；这是观测失败，
不是构建终态，不重新派发、取消或以新代码推送替换该运行。

### checkpoint 已提交，恢复探针 TLS 修复本机验证

checkpoint 错误分类修复提交 `67b31c2f` 已完成提交后 verifier 与四分片 703 项，耗时
272.506/477.971/328.523/753.739 秒；日志 `security-checkpoint-missing-post-*`。
尚未推送以免取消仍未取得终态的 image run `34284304697`。22:45:30 UTC 同一 run API
仍返回 rate-limit 403；匿名网页返回 404，不据此判断私有仓库的 CI 已停止。

恢复 TLS 回归首先证明旧实现复用源客户端 certificate/key，负向对照失败记录为
`security-restore-tls-before.log`。第一步分离服务端身份后，新的 CN=root 客户端用例又
揭示权限测试把已有证书管理员的连接当作匿名连接：期望拒绝的 Range 实际成功，记录为
`security-restore-tls-after.log`。两项失败均保留，不以最终修复结果覆盖原证据。

修复草稿现在使用仅限本地 loopback 的短期 serverAuth 身份；源客户端证书/私钥及 CN
保持原样，由恢复服务端继续核验源 CA。另生成 clientAuth-only、空 CN 的恢复专属身份，
供匿名与密码权限矩阵使用，防止源 CN=root 使拒绝测试失真。两份新身份均不是 CA，
只在这次恢复内部固定信任；源连接、生产 trust bundle 和源数据库权限均不修改。
所有临时身份文件 0600、目录 0700，随恢复目录在成功/失败后移除。无需生产 CA 私钥
或服务端私钥，不禁用双向 TLS，也不放宽 Snapshot/恢复超时或通过条件。

本机验证结果：

- 探针整个包普通测试通过，194.905 秒，`security-restore-tls-full.log`。
- 新 client-only 三成员恢复覆盖“恢复后启用 auth”和“已启用 auth、仅证书管理员”两种
  状态，以及生产规模数据、权限矩阵和既有复制/成员变更验证；race 通过，64.843 秒，
  `security-restore-tls-integration-race.log`。
- 实际 TCP/TLS 验证原 CN=root 与空 CN 传输身份，拒绝错误主机名、不可信/过期服务端、
  不可信或缺少客户端证书；还验证身份用途、非 CA、权限、配置缺失和无效源 CA。
  边界 race 连续 20 次通过，2.770 秒，`security-restore-tls-race.log`。
- vet 与 `go run honnef.co/go/tools/cmd/staticcheck@v0.8.1` 通过；首次直接调用
  `staticcheck` 因未安装可执行程序退出 127，不能把该次尝试算成功。后续指定版本检查
  的终态日志为 `security-restore-tls-staticcheck.log`。

隔离工作区 `/tmp/kubebrain-restore-tls.aiSeig/repo` 的四个源码文件已逐字节核对并用补丁
同步回主工作区。当前草稿尚未提交、发布或替换运行镜像；提交前 verifier 已通过 703 项，
四分片正在运行，日志 `security-restore-tls-pre-*`。真实集群仍固定 `339381af`，三副本
Ready；首轮综合失败仍是有效失败记录，修复探针的真实重测尚未执行。

上述隔离工作区的测试进程均已取得终态；四文件与主工作区逐字节一致后，已移除该临时
worktree 及空父目录。修复源码仍完整保留于主工作区，测试日志位于仓库外材料目录，
没有清理共享 Go 或镜像构建缓存。

### 修复探针真实重测准备

22:54 UTC 通过同一源码提交的 GraphQL statusCheckRollup 核验到 `build-and-push`
IN_PROGRESS，detailsUrl 精确对应 image run `34284304697` / job `102256177342`。
REST core 配额重置时间为 22:55:05 UTC，随后 REST 查询恢复，仍为同一 build/push 步骤
in_progress。没有重新派发构建或把限流视为失败。

仓库外 `kubebrain-security-smoke-restore-tls.template.yaml` 预留独立重测 Pod
`security-smoke-restore-tls`、fixture 前缀
`/kubebrain-rollout-availability/security-smoke-restore-tls/` 和 lease IDs
`2026090823000001/2026090823000002/2026090823000003`。目前仅生成模板、确认同名 Pod
不存在，未创建或执行。模板沿用已验证 `339381af` 不可变镜像，但等待上传单独编译的修复
probe，通过精确 SHA-256 后才运行；创建前必须填入已提交源码 SHA 与实际二进制摘要。
此方式是修复探针的诊断性集群重测，不是修复后发布镜像的验收，不能把基础镜像标签
误记成包含新修复。三副本服务、后端、证书 Secret、存储卷均不因此变更；不触及 secondary。

重测前再次核验 kube-system/测试 namespace UID 与原锚点一致，三个服务副本仍 Ready、
后端 3 PD + 3 TiKV 均 Ready、restart 0。固定 PD Pod UID 的只读 exec 适配 Region/存储
门禁再次通过：3 个连续 Region 样本无异常，证据 `security-restore-tls-region-gate.log`；
仍不声明原 API Service proxy 网络入口已经恢复。

本次只读对照 `/root/etcd` commit `5cd9f4ee13801e18825d661e5005ae599460bc3a`：
`server/etcdserver/v3_server.go` 的 AuthInfoFromCtx 先检查 token，再在 ClientCertAuth
启用时读取 TLS 身份；`server/auth/store.go` 的 AuthInfoFromTLS 使用已验证链叶证书 CN。
因此 CN=root 的无密码连接并非匿名连接，不能期待其在启用 auth 后被拒绝；这不是要求
KubeBrain 改变证书认证语义，而是修复恢复探针的身份隔离。参考源码没有改动。

恢复 TLS 修复提交前 verifier 与四分片 703 项已全部通过，耗时
268.762/498.608/331.407/753.403 秒，日志 `security-restore-tls-pre-*`。
下一步提交该批修复，立即执行提交后同样的 verifier/四分片，并从精确提交构建诊断探针。

### 修复探针已提交并启动诊断性集群重测

提交为 `669ac47033f1f2681c1f7a5e5cf3e10bc36c78c3`；提交后 verifier 已通过 703 项，
四分片正在运行，`security-restore-tls-post-*`。从该干净提交编译诊断探针，buildinfo
确认 Go 1.26.8、linux/amd64、CGO_ENABLED=0、vcs.modified=false、精确源码 SHA，
二进制 SHA-256 `1b1e204227e6684bb5cc7a9b963f3e25e42363f7103538f15db0133ac98354fc`。
本机暂存 `/tmp/kubebrain-restore-probe.YtiJH5/restore-probe`，待重测/清理结束后移除。
完整记录 `security-restore-tls-binary-buildinfo.log`、`security-restore-tls-binary-sha256.log`。
govulncheck v1.6.0 binary 扫描无可达及导入 package 漏洞；模块级仍报告
GO-2026-5932（x/crypto/openpgp，不在本探针调用路径），不宣称全部依赖零漏洞。
证据 `security-restore-tls-binary-scan.log` 与 `security-restore-tls-binary-scan-verbose.log`。

实际清单 `kubebrain-security-smoke-restore-tls.yaml` SHA-256
`9e85d348b6e074e3e2764bdd203d1c247b9a5ec030fe4814336f2b8876ab0746`，server-side dry-run
及非 root、只读根、无 SA token、无新增 PVC 等断言通过。创建前重新核验 cluster/StatefulSet
UID、三副本 Ready、旧 cleanup Pod 精确 UID+Succeeded+exit 0、同名新 Pod 不存在。
新 Pod UID `758d5c3f-f2c3-48c5-a848-22d72ee5b727`，worker3；上传先写 stage 文件，
核对摘要后 chmod 0500 并原子改名，入口再次验证摘要后才执行。
创建/状态/日志为 `security-restore-tls-smoke-created.json`、`security-restore-tls-smoke-latest.json`、
`security-restore-tls-smoke.log`。

23:08:21 UTC 三个直连审计已通过，Put/Delete revisions 为 356/357、358/359、360/361；
随后 PROBE_STARTED。当前综合重测尚无终态，不能把审计通过当作 Snapshot/恢复通过。
提交后本机四分片与这一独立诊断 Pod 并行；未推送取消 image run，未变更服务镜像或后端。
源服务证书认证五项回归 race 三轮通过（2.647 秒），记录
`security-restore-tls-source-auth-race.log`。

### 诊断重测因工作卷容量被驱逐，独占 fixture 已回收

新 probe 于 23:09:25 UTC Failed、exit 137，无 PROBE_SUMMARY。仅据 137 不能判断 OOM；
随后读取精确 Pod UID 的事件确认 23:09:23 Evicted：
`Usage of EmptyDir volume "work" exceeds the limit "512Mi".`，接着 Killing。
证据 `security-restore-tls-smoke-events.json`。完成后无法 exec 读取 cgroup，cadvisor
查询也未匹配该已退出容器；不编造内存峰值。worker3 当前 MemoryPressure/DiskPressure
均 False。此轮不算综合通过，不重跑相同容量配置以掩盖失败。

创建精确绑定失败 probe UID/前缀/租约的 cleanup Pod `security-smoke-restore-tls-cleanup`，
UID `da2cffd1-6b3d-44ce-8236-932f04b1c857`。23:11:34 UTC Succeeded、exit 0，输出
`FIXTURE_CLEANUP_OK status=recovered owner_uid=758d5c3f-f2c3-48c5-a848-22d72ee5b727 keys=529 users=0 roles=0 leases=3`。
已清除该失败测试的独占键与租约，没有用户/角色残留或其他租户数据删除。
日志 `security-restore-tls-cleanup.log`、终态 `security-restore-tls-cleanup-result.json`。
测试数据可由新一轮专属 fixture 重新生成，旧失败记录保持不变。

原 512 MiB 是此次环境手工探针清单的容量，不是产品恢复协议要求。真实恢复需要三个
成员、后续 voter/learner 数据目录及官方 WAL 预分配，还包含 Snapshot 与 47 MiB 上传
探针；不能仅按下载的 Snapshot 文件大小分配空间。下一轮使用新的 Pod/fixture/lease ID，
把工作 emptyDir 和容器 ephemeral-storage limit 显式配置为 2 GiB、request 为 2 GiB；
CPU/内存、原测试规模、超时和验证条件不变。不调整节点驱逐阈值，不修改 TiKV 或 etcd
WAL 实现，不把这次容量修正当作产品性能通过。三个服务副本保持原镜像和 Ready 状态。

恢复 TLS 提交 `669ac470` 的提交后 verifier/四分片 703 项全部通过，耗时
256.984/468.300/315.747/747.043 秒；前后两轮门禁均完成。

2 GiB 重测 Pod 为 `security-smoke-restore-space`，UID
`6a7035e2-07b3-4312-968e-8a8e2add2942`，前缀
`/kubebrain-rollout-availability/security-smoke-restore-space/`，lease IDs
`2026090823200001/2026090823200002/2026090823200003`。实际清单
`kubebrain-security-smoke-restore-space.yaml` SHA-256
`b2cbae6d62b9c11a5cf47b02d1b5cbd020154761fa5841ed06d02bb474b12ea9`，server-side dry-run
与新增临时存储 request/limit 精确断言通过；使用同一个 `669ac470` 修复二进制和同一个
已验证基础镜像，没有重新编译或改动验证逻辑。原失败 fixture 已回收后才创建新 Pod。
23:15:38 UTC 三个直连审计通过，Put/Delete revisions 为 641/642、643/644、645/646。
日志 `security-restore-space-smoke.log`，创建响应 `security-restore-space-smoke-created.json`。
当前重测仍在进行；启动早期工作目录 47 MiB、memory.current=91,598,848 bytes、
oom/oom_kill=0，证据 `security-restore-space-resources-initial.log`；该采样不是峰值。

### 修复后二次诊断重测通过

`security-smoke-restore-space` 于 23:17:15 UTC Succeeded、exit 0。最终输出：
`ok=300 fail=0 total=300 watch=300 direct_watch=300x3 lease=alive direct_lease=alive`
`range_stream=64 snapshot=1 stream_retries=0 stream_partial_retries=0`。
public/direct lease restart 均为零，max operation/direct latency 为 562 ms、Put 533 ms、
Watch-after-Put 93 ms、TSO 102 ms、Region 21 ms。完整日志及终态为
`security-restore-space-smoke.log` 与 `security-restore-space-smoke-latest.json`。
其中 Snapshot 成功计数在 checksum、官方 etcdutl restore、三成员启动、历史/规模数据、
权限矩阵及既有 quorum/voter/learner 验证全部完成后才增加，不只是下载成功。
后续采集工作目录容量时 Pod 已正常完成，exec 被拒绝；因此没有有效恢复峰值容量/内存
测量，不能用失败采集文件推断峰值。此前 512 MiB 驱逐记录仍保留。

这证明运行中的 `339381af` KubeBrain 三副本，在此次无故障注入场景下可通过修复后的
`669ac470` 探针综合验证；不等于包含修复的发布镜像已验收，不等于真实 HA 故障演练、
后端 TLS 轮换、长时间 soak 或完整生产就绪目标已完成。

独立 cleanup Pod `security-smoke-restore-space-cleanup`，UID
`d7088514-fc73-4614-980b-710adc21e0a3`，于 23:18:48 UTC Succeeded、exit 0，输出
`FIXTURE_CLEANUP_OK status=absent owner_uid= keys=0 users=0 roles=0 leases=0`。
确认成功探针自身已完成独占 fixture 清理，没有再执行额外数据删除；证据
`security-restore-space-cleanup.log`、`security-restore-space-cleanup-result.json`。

三轮探针及对应三个 cleanup Pod 共六个均已终止；在保存完整终态、日志、事件后，通过
Kubernetes DeleteOptions 的精确 UID + resourceVersion preconditions 删除这六个对象，
再次查询均不存在。身份清单 `security-smoke-terminal-pods-before-cleanup.json`；各次
删除请求/响应为 `delete-security-smoke-*.json` / `delete-security-smoke-*-result.json`。
同时核验摘要后删除本机 `/tmp/kubebrain-restore-probe.YtiJH5/restore-probe` 及空父目录。
本次删除的是专属已终止测试 Pod、其临时工作制品和本机编译程序；未删除 PVC/PV、
Secret、StatefulSet 或后端数据卷。源码、清单、摘要和日志均保留，程序可从提交重建；
临时 Snapshot 文件不作为备份保留。没有清理共享 Go/BuildKit 缓存。

### e156a9f7 镜像完成，准备快进发布恢复修复

image run `34284304697` 最终 completed/success，job `102256177342` 用时 1h16m17s。
发布验证 23:21:47–23:24:07 UTC 成功，promotion 23:24:07–23:24:14 成功，
整个运行于 23:24:44 UTC 更新为完成；未被后续推送取消。
源码为 `e156a9f7e79b2d5aa20214bc80d5ee2e73e68b28`，索引 digest
`sha256:b017e3421d670c7b0d9223601e9c75528eae7ddb4f579f3167d5f06c3e1a2185`，
amd64 子镜像 `sha256:3e7e26f8f4a009ef41f1cd2f3cee0e6756a33b2b470f6a384bbce98fdf0109ad`，
arm64 子镜像 `sha256:fd000dd82789b5bdf9632e52ff7baf7ac7314bd29537ea9a030047c658bfdb0b`。
registry 回读索引与 CI 成功状态均已核验；证据 `security-e156a9f7-image-manifest.json`、
`security-e156a9f7-image-ci.json`、`security-e156a9f7-image-ci.log`。
Runner 报告部分固定 action 使用 Node 20 声明并被强制以 Node 24 运行的弃用警告，
但本次步骤均成功；后续应独立升级 action，不以此警告掩盖发布失败。

该镜像尚不包含后来的 checkpoint `67b31c2f` / restore TLS `669ac470` 修复；不替换当前
已通过诊断测试的 `339381af` 三副本。当前准备将修复及其前后两轮 703 项门禁、真实重测
和清理记录一起快进推送 `dbaas`，由新 push 自动构建完整新镜像，不重复 workflow_dispatch。

已快进推送到 `60137eb36ee7e4a8f61251a6f91b36ae1135aa23`，远端 dbaas SHA 核验一致。
新 image run [34290666105](https://github.com/fivetime/kubebrain/actions/runs/34290666105)
由 23:26:37 UTC 的 push 自动触发，源码精确为该 SHA；当前 queued/in_progress，尚未取得
成功终态或新发布 digest。元数据保存在 `security-60137eb3-image-ci.json`。不因本条文档
追加再推送取消该构建；后续先跟踪同一 run，并核对发布产物，再安排使用完整新镜像的
部署/HA 验证。当前运行实例仍是先前固定 `339381af`，不能宣称已部署 `60137eb3`。

### HA 准备：生产健康契约与探针 TLS 信任隔离（2026-09-08）

准备受控 leader Pod 故障测试时发现，旧 rollout runner 只接受 `/readyz` 的 1 秒
profile，而实际两个生产清单使用 `/ready`、period 5 秒、timeout 6 秒、failure 3 次；
后者为服务端 5 秒健康检查预算保留返回时间，并非放宽业务 5 秒延迟 SLO。
两个生产清单均使用 HTTPS info，即使 public client 为明文；原 runner 从 client TLS
推导 info 协议也不正确。新增测试直接读取这两份生产 YAML 的健康/TLS 参数，旧实现
对两者均失败，证据 `security-rollout-contract-before.log`；修正后 targeted 测试通过。
第一次回归另暴露两个过时断言（旧客户端挂载路径、旧迁移 readiness path），已修正；
失败日志保留，不能把这两次失败记为成功。

本次改动要求 TLS 探针使用独立 `PROBE_CLIENT_TLS_SECRET`，拒绝复用源 StatefulSet
挂载的 Secret；本环境应指定现有 `kubebrain-test-client-tls`，而非服务器 TLS Secret。
probe、leader 发现、补偿 cleanup 都只使用该客户端身份。公开证书检查还确认 info
与 public client 由不同 CA 签发，原能力检查复用 public client TLS 会失败。
新增 `PROBE_INFO_CA_CONFIGMAP` / `--source-info-cacert`：仅给主探针挂载独立公开 info
CA，逐个验证 info endpoint SAN，不发送客户端身份，也不关闭 TLS 验证。
该 ConfigMap 尚未在真实集群创建；客户端私钥、CA 私钥、kubeconfig 不进入 Git。

四种 public/info HTTP(S) 组合通过本机真实 HTTP/TLS server 验证，测试还实际调用
探针 `run`，证明能力检查成功后才触达故意不可用的 PD，而非只测 TLS 构造函数。
info server 主动请求客户端证书时也未收到业务身份。未知 CA、错误主机名、证书
过期、缺失/无效 CA 文件及 HTTP 降级均拒绝。日志 `security-rollout-info-trust-targeted.log`
（0.084s PASS）；生产 runner 相关 targeted 日志
`security-rollout-contract-info-targeted.log`（41.747s PASS）。完整提交前后生产分片、
probe 全量与 race 检查尚待终态，不能据此声称已完成全量验收。

23:51 UTC 左右只读核验：KubeBrain 三副本及 PD/TiKV 各三副本均 Ready、restart 0。
未删除 leader Pod、未滚动服务、未改变后端或 StorageClass。本次仍只允许既有
`rook-ceph` 消费者集群；不得使用 `rook-ceph-secondary`。CI `34290666105` 仍运行，
不推送取消它；其中源码 `60137eb3` 不含本次新增 info 参数，后续需验证新的完整镜像。

#### 2026-09-09：真实 info 信任验证与最终门禁登记

只读适配器 `rollout-readonly-kubectl.sh` 仅放行本 namespace 的 controller/Service/Pod/
ConfigMap GET，拒绝 run、patch、delete、exec 及 UID 删除。实际 runner 通过源
StatefulSet、生产健康契约、headless Service 和三个 Pod 的初始读取后，在第一次
cleanup Pod 创建时按设计退出 1（`READONLY_AUDIT_DENIED verb=run`）。这只证明初始
部署契约可被识别，不是完整 rollout PASS；两个 audit Pod 回查均不存在。日志
`security-rollout-trust-readonly-audit.log`。同期 Region 门禁通过三次连续样本：
PD/TiKV 3/3、abnormal Regions 0；仍使用已记录的精确 PD Pod UID 只读 GET adapter，
并非修复了原 API Service proxy 网络路径。日志 `security-rollout-trust-region-gate.log`。

实际 info 证书由 `kubebrain-tk-001-003-info-ca` 签发，有效期
2026-09-08 18:51:12 至 2026-12-07 18:51:12 UTC。SAN 覆盖
`kubebrain-peer.kubebrain-dbaas-test.svc` 及完整 Pod 域名的 wildcard，但不覆盖 runner
拼接的短 Pod 域名，因此增加独立 `PROBE_INFO_TLS_SERVER_NAME`，本环境应设置：

```sh
PROBE_CLIENT_TLS_SECRET=kubebrain-test-client-tls
PROBE_INFO_CA_CONFIGMAP=kubebrain-test-info-ca
PROBE_INFO_TLS_SERVER_NAME=kubebrain-peer.kubebrain-dbaas-test.svc
```

`verify-info-trust-readonly.sh` 依次对精确 UID 的三个 KubeBrain Pod 建立本机回环
port-forward，访问 `/capabilities`：错误的 public client CA、短 Pod SAN 均返回
curl 60；正确独立 info CA + 明确 Service SAN 则三者全部通过，返回
`snapshot-history-pin-before-write-barrier-release.v1`。未使用客户端证书或私钥，
未使用 insecure 选项；前后 Pod UID 不变。该验证经过 API port-forward，不证明
Pod 网络内 DNS/NetworkPolicy 路径或新探针发布镜像已验收。
证据 `security-info-trust-readonly-result.log`、`security-info-trust-{0,1,2}-capabilities.json`、
`security-info-trust-short-name.log`、`security-info-trust-wrong-ca.log`。所有 port-forward
已停止，本机 18880 端口回查无监听；没有临时编译新二进制。

已创建专属 ConfigMap `kubebrain-test-info-ca`，UID
`d8ae8541-93db-426b-8231-a18cae709990`，immutable=true，仅有 `ca.crt`。
内容 SHA-256 `3d478abb4ddfd850b7cd6755f6a8c7bd148776e58fcda03550e8dd6b18025d45`，
与本机公开 CA、源 info Secret 的公开 CA 均相同；回读 UID/键集合/摘要已核验。
证据 `security-info-trust-configmap-created.json`。本次只新增该公开信任材料，未修改
任何服务器证书、私钥、StatefulSet、PVC 或后端配置；ConfigMap 留给后续升级测试使用。

probe 全量测试 `security-rollout-trust-probe-suite.log` 为 PASS（175.357s）；初次
info TLS race 20 轮 PASS（3.259s）。加入显式 info 服务名后，再跑 20 轮 race，
包括正确 SAN override 成功、错误 override 拒绝，PASS（3.708s，
`security-rollout-trust-server-name-race.log`）；相应 runner targeted PASS（12.409s）。
初轮 vet/staticcheck 均 exit 0，最终版检查另记 `security-rollout-trust-final-{vet,staticcheck}.log`。

修改服务名传参前启动的 `security-rollout-trust-pre-{0,1,2,3}.log` 只属于中间草稿，
不能作为最终提交门禁。最终源码已重新执行 `hack/production/test-shard.sh --verify 4`：
708 项，分桶 170/194/181/163；四个并行分片运行记录为
`security-rollout-trust-final-pre-{0,1,2,3}.log`，当前等待完整终态。
提交前必须全部成功；提交后立即再次 verifier + 四分片。尚未提交或推送本次产品代码，
也未执行真实 HA。CI `34290666105` 仍在 Build and push 步骤，继续观察同一 run，
不得另一次 push 将其取消；目标整体仍未生产就绪。

#### 提交前门禁发现启动失败测试的计时范围问题

`security-rollout-trust-final-pre-0.log` 为 FAIL：
`TestRolloutAvailabilityRunnerReportsProbeFailureBeforeStartBarrier` 的整条 runner 调用耗时
5.146559108s，超过原测试的 5 秒墙钟断言；正确 `PROBE_FAIL` 和立即中止信息已经
出现。完整调用还包括准备、两轮所有权约束 cleanup 和多次 Kubernetes mock 子进程，
并不等于数据面的 Put/Watch 延迟。检查实际 shell 控制流确认，第一次日志读取发现
终态失败后直接 exit，并非继续等待 60 秒启动屏障。相同源码单项重复五轮 PASS
（20.198s，`security-rollout-trust-start-failure-timing-recheck.log`），但不能以重跑
掩盖原失败；并行负载影响只是与证据一致的解释，不作为已证明的唯一原因。

测试改为显式验证只发出一次主 probe 日志请求、保留原失败原因、没有转成等待屏障
超时、没有 StatefulSet 修改；使用现有进程组 helper 的 10 秒整条测试命令兜底，
避免超时留下子进程。业务 5 秒延迟 SLO、runner 运行预算及失败处理逻辑均未改动。
这比用整个 setup/cleanup 耗时判断是否重复轮询更直接。新测试十轮回归记录为
`security-rollout-trust-start-failure-semantic.log`，随后还需完整重新执行 verifier 与
四分片；先前 `final-pre` 这一轮不能记为完整 PASS，也不能据它提交产品代码。

十轮语义回归已 PASS（40.479s）。上一轮 `final-pre` 的其余分片 1/2/3 最终
PASS（493.896/330.835/765.376s），分片 0 仍记录 FAIL，不与另一轮成功结果拼接。
确认旧分片进程全部终止后，重新启动整轮提交前门禁，日志前缀改为
`security-rollout-trust-commit-pre-`；五个改动源码/测试文件的 SHA-256 清单保存在
`security-rollout-trust-commit-code.sha256`，提交前再次核对，防止用旧源码测试结果
覆盖后续修改。本轮全部成功后才允许提交，随后立即运行提交后 verifier/四分片。

探针 govulncheck v1.6.0 扫描 Go 1.26.8 与 80 个模块：可达漏洞 0、已导入包级
告警 0；模块层面仍报告 `GO-2026-5932`（`golang.org/x/crypto/openpgp` 无维护且
unsafe by design，没有修复版本），探针不导入该包。不能将模块告警删除或笼统宣称
依赖图零告警；原始与详细日志分别为 `security-rollout-trust-probe-vulnerability.log`
和 `security-rollout-trust-probe-vulnerability-verbose.log`，两次扫描 exit 0。

#### 后续 HA 验收边界：主动释放与无清理故障不同

只读回查真实 StatefulSet 与 production TLS 清单均设置 election lease/renew/retry
为 30s/25s/500ms。`pkg/server/service/leader/leader.go` 使用 `ReleaseOnCancel`，
并提供 `EnsureVoluntaryRelease` 与 `WaitForVoluntarySuccessor`；源码明确说明，主动
释放失败会让继任者等待完整租约。因此即使受控 Pod 删除通过，也不能据此推断真正
无机会清理的进程故障、节点故障或网络分区可在 5 秒内恢复。具体故障窗口仍需实际
证据，不能仅从配置推出每次都会等待恰好 30 秒。

本轮不调低选举/自我隔离预算，也不调高业务 SLO 使门禁通过；后续应记录旧进程是否
执行主动释放、选举 term 变化、客户端完整恢复窗口及新 Pod 初始化耗时。受控 Pod
删除不替代跨节点/分区验收；共享 worker 重启、隔离或后端变更仍不在本次操作范围。
选举与资源锁本机 race 检查另记 `security-rollout-trust-election-race.log`，不是实际
分布式故障证据。当前仍无真实 HA 注入。

leader/resource-lock race 检查已 PASS（2.519s / 1.162s）。额外 probe 全量 race
首轮 `security-rollout-trust-probe-full-race.log` 为 FAIL（255.448s）：既有
`TestExternalFixtureCleanupRecoversWithoutPublicOwnershipKey` 在嵌入式官方 etcd
创建 reader 用户时达到每操作 3 秒 deadline。失败处属于认证 fixture 准备，不是
本次 info TLS 分支；未发现 race detector 数据竞争报告，但这不使超时结果变成成功。
不更改 fixture/认证配置或生产超时，原测试单项 race 五轮 PASS（80.204s，
`security-rollout-trust-fixture-race-recheck.log`）。负载/调度影响尚未被证明为唯一原因，
因此另外重跑相同完整 race 命令，记录为 `security-rollout-trust-probe-full-race-recheck.log`；
其终态仍需核验，保留第一次失败作为稳定性证据，不宣称从未失败。

#### 本轮提交前完整门禁通过（2026-09-09 00:35 UTC）

`security-rollout-trust-commit-pre-verify.log` 确认 708 项、四个非空分片
170/194/181/163；本轮四分片均 exit 0，耗时分别
268.456/489.163/319.507/767.412s，日志 `security-rollout-trust-commit-pre-{0,1,2,3}.log`。
五个改动源码/测试文件摘要再次全部匹配开跑清单；没有用旧分片或单项重跑替代这轮全量。
最终 vet、bash 语法与 diff whitespace 检查通过。完整 probe race 相同命令复测
PASS（258.584s），记录为 `security-rollout-trust-probe-full-race-recheck.log`；这不
撤销前述一次认证 fixture deadline 失败，测试稳定性仍须持续观察。

现在准备提交本轮健康契约、客户端身份与独立 info trust 修复及证据。提交后立即执行
同一 verifier 和四个并行分片；此刻尚未有提交后结果。CI `34290666105` 最新仍为
in_progress / Build and push，本机提交不会取消它，暂不 push。真实集群仍为先前
`339381af`，没有 HA 注入；已创建的公开 info CA ConfigMap 保留给后续升级测试。

本轮产品代码已提交为 `4fd0d599bf72dad831848ce3b6ceb45a0f3b9515`，工作树提交后干净。
提交后立即执行 `hack/production/test-shard.sh --verify 4`，仍为 708 项、分桶
170/194/181/163；四个并行分片已启动，日志为
`security-rollout-trust-post-{0,1,2,3}.log`，verifier 为 `security-rollout-trust-post-verify.log`。
目前尚无四分片完整终态，不得先宣称提交后全量 PASS。此登记为文档追加，不改产品
源码或中断测试；尚未 push，也未触发另一条会取消 `34290666105` 的 image run。

### rollout trust 提交后全量通过，等待发布验收（2026-09-09 00:49 UTC）

产品提交 `4fd0d599` 的提交后 verifier/四分片全部 exit 0：708 项、分桶
170/194/181/163，耗时 270.004/480.024/320.514/751.929s。五个源码/测试文件
摘要再次匹配；正式前后两轮均完整通过。中间草稿失败、计时断言修正和一次额外
probe full race 超时记录仍保留，不能把历史记录改成从未失败。
确认所有工具会话终止后才生成 `security-rollout-trust-post-complete.json`，包含
产品 SHA、分片编号/项数/耗时及日志路径，供本环境后续测试前置检查使用。

CI `34290666105` 仍为 in_progress / Build and push。不可变源码标签
`dbaas-60137eb36ee7e4a8f61251a6f91b36ae1135aa23` 已能读到 OCI 索引：
`sha256:86ba2d77e6235f2e6b2596c1134f2613328dd40eff068458dd469d27ae64f060`，
amd64 `sha256:8908cce0403450742306d661679a978a307d4fa809166825dd643788768b2f5b`，
arm64 `sha256:23c0b1191fef2056d7375044213a929287c8b2d52f9e7b21d1095889535e7398`。
已通过本地平台解析 helper 排除 attestation 描述符，但还没有 CI 发布验证成功终态，
不得使用它进行部署/HA，不能当作当前服务镜像。证据
`security-60137eb3-image-{manifest,index}-pending.json`。

在仓库外准备 `run-security-controlled-ha.sh` 与 `uid-delete-test-cluster.sh`，但未
执行任何故障注入。入口要求本次完整 post 回执、CI success、已核验的精确镜像回执，
并核对 namespace/StatefulSet/TidbCluster/StorageClass UID、当前服务镜像/Ready/revision、
consumer `rook-ceph` CSI/clusterID/Secret namespace 以及 Region 健康；UID 删除 helper
固定显式 kubeconfig，不依赖本机默认 context。脚本保留 5 秒 public SLO、30 秒 direct
恢复上界与 5 秒 lease TTL，只针对动态确认的 KubeBrain leader Pod，不改变服务镜像、
后端或共享 worker。post 回执尚缺时已实际验证入口 exit 1，并在集群修改之前停止，
日志 `security-controlled-ha-guard-before-ready.log`。镜像回执尚未创建。

后续若使用 `60137eb3` 镜像运行该 hard-failover probe，它已经包含 restore TLS 修复，
但不含新 info CA 参数；hard-failover 模式不请求在线镜像升级，也不传 source info
参数，因此这不能替代新版本在线升级门禁验证。真正在线升级仍须等待包含 `4fd0d599`
的完整镜像发布。当前服务仍固定 `339381af`，尚未执行 HA；不推送打断现有构建。

### 60137eb3 镜像验收完成，开始限定范围的 HA（2026-09-09）

CI `34290666105` 于 00:56:59 UTC completed/success，job 用时 89m54s。
镜像验收 00:54:11–00:56:23 成功，promotion 00:56:23–00:56:29 成功。
最终不可变索引与两种架构 digest 均与上述 pending 记录一致；成功日志中的镜像
reference、实际 linux/amd64 与 linux/arm64 `Git SHA: 60137eb3...`、Go 1.26.8、
版本 `0.0.0-dbaas-60137eb36ee7` 以及 registry 回读已核对。
`dbaas` 标签回读也指向 `sha256:86ba2d77...ae64f060`，但实际测试仍只使用完整不可变 digest。
证据为 `security-60137eb3-image-ci-completed.{json,log}`、
`security-60137eb3-image-manifest-verified.json`、`security-60137eb3-promoted-image.json`；
据这些终态与镜像证据才生成 `security-60137eb3-image-verified.json`。

现在使用上述已验收 `60137eb3` 探针镜像与本机 `4fd0d599` runner，测试仍在运行的
`339381af` 三副本。由 launcher 再次读取 namespace/StatefulSet/TidbCluster/StorageClass
身份与 Region 健康，runner 动态重复确认 leader 后，以 UID/resourceVersion 限定删除
该单一 Pod。此测试不更换 KubeBrain 服务镜像，不请求 worker 重启或网络隔离，也不
修改 PD/TiKV。主日志预留 `security-controlled-ha-60137eb3.log`；此登记不代表故障已经
触发或测试已经成功，必须以实际 `HARD_FAILOVER_STARTED`、summary、恢复及清理终态为准。

完整 CI 日志还解释了本轮长等待：镜像于 23:55:29 已推送，GitHub Actions cache
export 步骤 `#170` 从 23:54:04 到 00:54:09，耗时 3606.3s（准备 404.0s、发送
3202.3s），随后才进入镜像验收。这是缓存导出瓶颈的直接日志证据，而非推断编译
耗时一小时。先检查更合适的缓存后端，再与已有修复一起推送；不能为提速移除镜像
安全扫描、双架构检查或发布验收。

### 受控 leader Pod 故障通过与 PD 卷策略修复（2026-09-09）

`security-controlled-ha-60137eb3.log` 已终止且 exit 0。使用已验收的 `60137eb3`
探针镜像及本机 `4fd0d599` runner，对仍运行 `339381af` 的三副本进行受控删除。
动态确认的 leader `kubebrain-2`（旧 UID `fcec128c-edce-4689-9576-8fd85c820864`）
被 UID/resourceVersion 限定删除；继任 leader 为 `kubebrain-1`，member ID
`1284742340`。StatefulSet 重建的 Pod UID 为 `334eae03-1f53-4eb5-93c7-e230bec2b50b`，
01:03:50 创建，01:04:08 Ready；这约 18 秒是 Pod 初始化时间，不是公共客户端中断时间。

完整结果：900/900 操作成功，public watch 900、三个 direct watch 各 900；public
最大延迟 1908ms、Put 786ms、Put 后 watch 1122ms，direct 最大延迟 13363ms。
public lease 一直存活且重连 0；direct lease 存活、重连 3 次、最大恢复 7550ms。
Range stream 170、snapshot/官方 etcdutl restore 验证 1，stream retries 3，其中 partial
retry 1。5 秒 public SLO、30 秒 direct 上界和 5 秒 lease TTL 未放宽。早期三次 Put
Unavailable 重试保留在日志中，`fail=0` 不表示底层没有瞬时错误。

故障前后 fixture cleanup 均为 absent，keys/users/roles/leases 全零；探针 Pod、清理
Pod 和 owner ConfigMap 已自动清理。旧 Pod 的两份 ephemeral scratch PVC/PV 随控制器
回收并重建，不是后端数据卷；没有手工删除 PD/TiKV 卷。服务镜像、runtime digest 与
StatefulSet revision 均未变化。最终九个前后端 Pod Ready/restart 0，PD/TiKV Pod UID
保持不变；Region gate 再次连续三样本通过，3 PD/3 stores/0 abnormal regions。
证据另见 `security-controlled-ha-final-{pods,pvc,pv}.json` 和
`security-controlled-ha-final-region-gate.log`（仓库外测试状态目录）。

这仅证明受控 leader Pod 删除场景。未获得旧进程是否主动释放租约的证据，不能等同于
无清理机会的 SIGKILL、节点失联、网络分区或跨可用区故障；也不能替代包含 `4fd0d599`
的新镜像在线升级验证。当前服务仍为 `339381af`。

存储复核发现既有配置偏差：TidbCluster `kb` 已声明 `pvReclaimPolicy: Retain`，但三份
PD PVC/PV 仍带旧 instance 标签 `kubebrain-test`，实际 PV 策略为 Delete；三份 TiKV PV
已经为 Retain。这不是本次 HA 引入的偏差。TiDB Operator v1.6.5 的
[reclaim policy manager](https://github.com/pingcap/tidb-operator/blob/v1.6.5/pkg/manager/meta/reclaim_policy_manager.go)
按集群名 `kb` 选择 PVC，旧标签使其遗漏这些 PD 卷。

在核对 namespace/TidbCluster/Pod/PVC/PV UID、绑定关系及 consumer rook-ceph 身份后，
仅对 `pd-kb-pd-{0,1,2}` 及其三个绑定 PV 的 instance 标签实施 UID/RV/旧值限定 patch，
由 Operator 自行收敛策略；没有手工覆盖 reclaim policy、修改 StorageClass 或重启后端。
`security-pd-volume-label-repair.log` exit 0，终态
`PD_PV_RETENTION_RECONCILED retained=3 source=operator unchanged_volume_identity=true`。
三份 PV 前后除 reclaim policy 外的完整 spec 与 UID 均相等（包括 CSI volumeHandle、
claimRef、容量与 StorageClass），仍为 Bound；九个服务 Pod 再读均 Ready/restart 0，
后端 UID 不变。证据 `security-pd-label-{0,1,2}-{pvc,pv}-before.json`、patched 回执及
`security-pd-label-{0,1,2}-pv-after.json`。此结果不改变 scratch 卷的 Delete 策略。

### CI 缓存迁移验证（2026-09-09）

针对 run `34290666105` 的 3606.3s GHA cache export，image workflow 改为向同一 GHCR
仓库的独立 `buildcache-dbaas` tag 导出 registry max-mode cache；保留旧 GHA cache
只读导入作为迁移回退。缓存 tag 与不可变源码镜像及正式 dbaas tag 分离，保留双架构、
provenance、SBOM、安全检查、镜像验证和验证后 promotion；不忽略缓存或构建错误。

新增结构化 YAML 回归锁定缓存参数、独立 tag、登录/构建/验收/晋升顺序与不可吞错。
原 workflow 上测试 RED，迁移后完整 `go test ./build -count=1 -timeout=5m` PASS
（0.392s），actionlint v1.7.12 与 diff 检查 PASS；日志
`security-registry-cache-{before,build-tests,actionlint}.log`。实际提速尚待新 CI 实测，
不能把配置改动当成性能验收成功。提交前后生产四分片结果另行登记。

本轮提交前 verifier 确认 708 项、分桶 170/194/181/163；四个并行分片均 exit 0，
耗时 270.010/487.354/336.249/771.301s，日志
`security-registry-cache-pre-verify.log`、`security-registry-cache-pre-{0,1,2,3}.log`。
这些结果未借用上一产品提交的分片；本轮仅修改 workflow 与对应 build 回归测试，
未修改已通过真实 HA 的 runner/probe。提交后须再次执行完整 verifier 与四分片。

缓存迁移产品提交为 `ee490c0f792e4c695bdff9948e8409ac2a12c6b1`。提交后立即运行
verifier，仍为 708 项、分桶 170/194/181/163；四个并行分片已经启动，日志
`security-registry-cache-post-{0,1,2,3}.log`，verifier 日志
`security-registry-cache-post-verify.log`。本次登记时四个工具会话仍在运行，尚未获得
提交后完整 PASS，也未 push；不可仅因日志文件存在而生成完成回执。
会话对应关系为 shard 0=28490、1=97582、2=8512、3=23459，后续应先轮询原会话，
不要因输出间隔长而重复启动。最后一次 image CI 查询仅有 completed/success 的旧 run，
下一步须等本轮 post 四片全部 exit 0，再 FF push 一次并核验新 run 的精确源码 SHA。

后续存储门禁缺口：`validate-tikv-region-health.sh` 已检查 Ready Pod、Region 健康、
PVC/PV Bound/claimRef、CSI 唯一身份及容量/磁盘压力，但没有检查
`persistentVolumeReclaimPolicy` 或控制器所依赖的 PVC instance 标签；对应 fake PV
甚至省略 reclaim policy 也能通过。故前述修复前 Region PASS 不能证明卷保护策略正确。
下一轮应增加 PD/TiKV 数据卷 Retain 的负例回归与拒绝逻辑（不应影响可回收 scratch 卷），
同时考虑旧 PVC 标签漂移诊断。当前真实六份后端 PV 再读均为 Bound/Retain，CSI clusterID
均为 consumer `rook-ceph`；仍不能把已修复一个环境等同于关闭产品门禁缺口。

### 数据卷保留门禁与清理重试回归（2026-09-09）

缓存提交 `ee490c0f` 的 post 四片现已全部终止：0/2/3 PASS，分别为
285.368/334.389/770.482s；shard 1 FAIL（484.716s）。失败为
`TestRolloutAvailabilityRunnerRetriesTransientFixtureUIDDeleteFailures`：在测试专用
3 秒删除预算内报 fixture cleanup Pod still exists，不能把这一轮登记为全量通过。
原测试不改代码单跑五轮 PASS（26.704s），日志
`security-registry-cache-fixture-recheck.log`；调度/负载尚未被证明为唯一原因。
没有生成 cache post 成功回执，未 push，也未重新触发 image CI。

该正例验证一次 UID/RV 删除失败后的重读与精确重试次数，不是 3 秒清理 SLO。
修订后采用现有生产清理预算 60 秒，同时用共享 helper 对整个测试进程组设 30 秒上限，
保留两类对象各恰好两次删除的断言，失败时附完整 fake kubectl trace。
生产 runner、业务 5 秒 SLO、lease TTL 均不变；持续删除失败时 3 秒超时并回滚的既有
负例不变。修订正例十轮 PASS（56.194s），日志
`security-retention-fixture-retry-targeted.log`；仍须完整提交前后门禁验证。

为不影响仍在运行的旧 post 分片，在临时 worktree 准备新存储回归。原脚本面对
`pd-kb-pd-0` 的 Delete PV 返回成功，负例真实 RED（3.310s），日志
`security-retention-before.log`。新检查要求每个实际绑定的 PD/TiKV 数据 PV 明确为
Retain，独立于 CSI 绑定/唯一身份/容量检查，错误包含组件、Pod/PVC/PV、期望与实际值。
回归覆盖两类组件各三个 ordinal，以及 Delete/缺失/null/Recycle/空值/大小写错误/
数组/对象/bool/number 共 60 个负例。原有完整健康与边界用例保留，repair 集成 fixture
补齐生产要求的 Retain 字段；没有跳过修复脚本的存储检查。

首次草稿目标组 FAIL（253.210s）：基础健康用例报告 shell unexpected EOF；该轮执行
期间曾编辑脚本的 local 声明，输入未冻结，不能作为固定版本验收。保留
`security-retention-targeted.log`，不把草稿错误改记为 PASS。停止全部相关旧会话后，
四个最终源码/测试文件转回主工作树，并逐字节比对临时副本，再记录
`security-retention-code.sha256`；临时 worktree 与其空父目录已删除，改动均保留在主仓库。
固定版本的完整目标组已重新启动，日志 `security-retention-targeted-final.log`，此刻
尚无终态。bash 语法、diff 与 vet 已 PASS。

相同最终门禁脚本已在真实测试集群只读运行 PASS，日志
`security-retention-real-region-gate.log`：3 PD/3 stores、连续三个健康 Region 样本，
六份 Bound/Retain 数据卷通过检查。仍使用固定 kubeconfig 的 PD exec 只读适配器，
该结果不表示原 API Service proxy 网络路径已修复。没有删除/重启后端、修改卷或
更换消费者 StorageClass，也没有执行在线服务升级。

固定输入的目标组已终止且 PASS（293.222s），包括 60 个数据卷策略负例、原健康与
响应边界检查；日志 `security-retention-targeted-final.log`。未改动的删除持续失败回滚
负例与 release gate 测试 helper 检查一同 PASS（11.481s），日志
`security-retention-failclosed-targeted.log`。完整 build 包再次 PASS（0.399s），四个
改动源码/测试文件摘要全部匹配冻结清单。现在启动本轮完整提交前 verifier 与四分片，
总数 709，分桶 171/194/181/163；日志前缀 `security-retention-final-pre-`。尚未有这轮
完整结果，不以定向检查替代全量，不与失败的 cache post 轮拼接，也尚未提交新修复。

本轮 pre verifier 已 exit 0；四个原会话对应 shard 0=99536、1=21197、2=92992、
3=89046。后续先轮询这些会话，不重复启动，不在测试期间编辑已冻结的四个源码/测试
文件。仅全部 exit 0 且摘要仍匹配后才提交，提交后立即再次 verifier/四分片；通过后
再 FF push 并跟踪新 image CI。当前 HEAD 仍为 `4e0c6948`，本轮四个代码/测试文件与
两份文档尚未提交，服务仍是 `339381af`。

### 等待门禁期间的只读冷启动复核（2026-09-09）

本轮未修改冻结源码、后端配置或部署状态。补读实际继任 leader `kubebrain-1` 的日志：
01:03:51.377727 became leader；compact/quota/lease/event_log 初始化分别为
1.892289/5.455233/5.963226/43.446209ms，checkpoint 为 81.899223ms，完成于
01:03:51.980898。证据 `security-controlled-ha-successor-initialization.log`。此前记录
的约 18 秒是新建 follower Pod 的生命周期，不能替代上述 leader 初始化计时；这里也
没有证明旧进程是否主动释放租约。本次已有数据集的快速接管不关闭首次创建 watermark
时的 10m26s 冷启动缺口。

复核 TiKV v8.5.3 的
[GetStoreSafeTS handler](https://github.com/tikv/tikv/blob/v8.5.3/src/server/service/kv.rs)
与 [range safe-ts worker](https://github.com/tikv/tikv/blob/v8.5.3/components/raftstore/src/store/worker/check_leader.rs)：
RPC 读取重叠 Region 的安全水位，不等于请求即时推进水位。本机 fork 的范围查询确实
逐个请求已筛选 Store，不是直接复用客户端后台 minSafeTS 缓存。当前只读查询 store 0
配置仍为 resolved-ts.enable=true、advance-ts-interval=20s、hibernate-regions=true、
peer-stale-state-check-interval=5m；未将这些配置与历史延迟简单等同为因果关系。

三个 Store 的当前 follower safe-ts gap 样本分别为 32048/22740/32984ms，
concurrency manager min lock ts 均为 0；store 1 累计 fail_advance_count{reason=lock}
为 945。证据 `security-coldstart-audit-store-{0,1,2}-verified.log`。它们是当前 Store
级指标，不是历史冷启动时或本项目精确 key range 的水位证据；累计计数不能单独证明
过去那次延迟由锁导致。最初指标筛选命令因正则转义错误退出，修正筛选后才得到上述
有效样本，未把工具错误视为 TiKV 故障。

另有待验证的客户端边界：`GetTiKVStoreSafeTSForRange` 将调用方范围直接放入 RPC；
API v1 `EncodeRequest` 只附加 context，而服务端 `StoreMeta.search_region` 按 Region
元数据边界查找。现有 mock 在 RPC codec 之前断言原始 start/end，不能代替真实线协议
编码测试。下一步应以 memcomparable Region 边界建立最小回归，确认是否存在 raw key
与 Region key 混用，覆盖空边界、API v1/v2 和跨 Region 范围；在证明前不声称它是此次
冷启动根因，也不跳过 safe-ts/readiness 或放宽一致性契约。

本轮最终提交前四分片全部 exit 0：709 项、171/194/181/163，耗时分别
458.868/461.802/306.852/734.421s；日志
`security-retention-final-pre-{0,1,2,3}.log` 与对应 verifier。冻结摘要再次全部匹配，
不包含其它失败轮的替代分片。现在提交本轮数据卷 Retain 门禁、回归与清理重试正例
修订，随后立即执行同一 verifier 与四个提交后分片；此刻尚无提交后完整结果。

产品提交已完成：`e83d95b276bc965d6eb3ba8dd53eee199dc7481c`，提交后工作树干净。
立即执行 post verifier 已 PASS（709，171/194/181/163），四个并行分片已启动，日志
`security-retention-post-{0,1,2,3}.log`，verifier 为
`security-retention-post-verify.log`。原会话 shard 0=47364、1=61493、2=78420、
3=63756；后续先轮询这些会话，只有四片终态均 exit 0 才能生成完成回执及 FF push。
当前没有提交后全量通过声明，也未推送或启动在线升级。本段为文档追加，不改变冻结
源码，不应中断或替换当前测试。

### Retain 门禁完整通过，发现并修复客户端范围编码缺陷（2026-09-09）

`e83d95b2` post verifier 与四分片全部 exit 0：709 项，171/194/181/163，耗时
459.841/467.934/310.912/744.853s。四个冻结文件摘要再次全部匹配；据终态才创建
`security-retention-post-complete.json`。这轮完成 Retain 门禁前后验证，但不撤销
之前 cache post shard 1 的失败记录。暂缓 KubeBrain push/在线升级，先纳入以下
新确认的一致性边界修复；没有启动会被后续推送取消的 image CI。

在独立 `/root/tikv-client-go`（原基线 `b5b63af1`）新增 codec 最小回归，验证 API v1
raw/txn 与 API v2 raw/txn：v1 txn 和 v2 两种模式真实 RED，编码后的请求会漏掉请求
范围对应的 Region。原 KVStore mock 位于 RPC codec 之前，旧测试未覆盖这一层。
进而使用真实 loopback gRPC server、按 TiKV v8.5.3 半开 Region 交集规则取最小非零
safe-ts，在独立 baseline worktree 上复现：目标 Region 水位 50，旧请求却得到相邻
Region 的 900（三种模式均 FAIL，v1 raw 保持 identity 编码）。证据
`security-client-safets-codec-before.log`、`security-client-safets-transport-before.log`。
这不是已经在真实 TiKV 多 Region 集群上完成的回归；也尚未证明它是早前冷启动延迟
的直接原因。基线 worktree 只增加同一回归测试，结束后比对并删除，测试保留在 fork。

修复只作用于 StoreSafeTS 请求：使用 Region memcomparable 编码，v1 空/空继续保留
store-wide 语义，v2 空或 nil 范围限制在自身 keyspace；克隆结构，保持重试不修改或
重复编码原请求。新增 nil/空边界、单边范围、二进制键、8 字节边界回归，gRPC 还验证
不会误纳入安全水位更低的相邻 keyspace。完整 codec 测试 PASS（0.031s），相关
codec/transport race 十轮 PASS（1.138/1.752s）；完整模块普通/race（`-p 2`、无过滤）、
build/vet/mod verify 全部 PASS，govulncheck v1.6.0 为 No vulnerabilities found。
日志前缀 `security-client-safets-`，四个文件摘要为 `security-client-safets-code.sha256`。

独立 fork 的修复已本地提交 `9fe67f4`，立即执行完整提交后普通及 race，日志
`security-client-safets-post-{unit,race}.log`，串行工具会话 48172；此刻尚无两轮完整
终态，也未 push fork。KubeBrain go.mod 仍固定远端 `b5b63af1` 的 pseudo-version，
没有改为本地 replace。下一步先完成 fork 复验/快进推送/CI，随后更新远端不可变模块
版本并执行 KubeBrain 依赖变更的完整前后门禁，最后再触发镜像发布与真实升级验证。
新问题修复前不得以此前 HA PASS 代替范围编码验收；当前服务镜像仍未由本轮更换。

客户端 `9fe67f4344a00be3c05aeefed14e618c15ebb0e9` 首次 post 普通测试 PASS，但
完整 race FAIL：既有 `TestBackoffErrorType` 在 witness 随机退避累计 996ms 成为最长
时仍硬编码期待 txnNotFound 错误。生产 `longestSleepCfg` 返回 witness 错误符合规则，
该失败不是 race detector 数据竞争报告。原测试十轮单项 race PASS（34.890s），
`security-client-safets-backoff-recheck.log`，不撤销首次 full race 失败。

新增测试层确定性修订：只向 Backoffer 注入固定 sleep 返回值，保留生产累计、排除
server-busy、预算判定与错误选择逻辑，分别断言 transaction/witness 为最长时的正确
错误；不改生产随机 jitter、超时或重试参数。完整 retry 包 100 轮 race PASS（3.294s），
日志 `security-client-safets-backoff-fixed.log`。当前重跑完整模块普通/race 提交前检查，
日志 `security-client-safets-final-pre-{unit,race}.log`，原会话 43103。五个 Go 文件
摘要为 `security-client-safets-release-code.sha256`；独立 fork 只有这份测试修订及
维护文档未提交，尚未 push。下一步全量通过后提交该测试修订、立即完整 post 复验，
再发布 fork 和核验 CI。KubeBrain 的 709 项前后门禁已完成，但依赖仍未更新。

确定性测试修订已提交为 `832b70fd622f20d39a82ce6b50d3938a94b8bbd3`，不改
`9fe67f4` 的运行代码。最终完整 pre 和 post 普通/race 两轮均 PASS；post 日志
`security-client-safets-final-post-{unit,race}.log`，原会话 28258 已 exit 0。
五个文件摘要再次全部匹配，vet 复核 PASS。此前随机测试失败仍保留，未使用单项
结果替代最终完整回归。然后将两个提交从 `b5b63af1` 快进推送到
`fivetime/tikv-client-go: kubebrain-v2.0.7`，没有 force push 或上游 PR。

自动触发 fork CI [34303780336](https://github.com/fivetime/tikv-client-go/actions/runs/34303780336)，
head SHA 精确为 `832b70fd...`。目前 in_progress，test job `102316096965` 与 security
job `102316097209` 均在 Set up Go 阶段，不能先登记 CI success；不要重复推送取消该 run。
Go 实际解析远端 commit 返回
`v2.0.8-0.20260909023231-832b70fd622f`，Origin.Hash 与上述完整 SHA 一致。
KubeBrain go.mod/go.sum 此刻未改变，仍锁定 `b5b63af1`；下一步先读取原 CI 终态，
成功后更新远端 replace 至此不可变版本、核验模块摘要与应用测试，并重新执行依赖
变更的 709 项提交前后门禁，之后再发布新 KubeBrain 镜像。服务端/后端均未因本轮
客户端修复而部署或重启。

### 远端客户端验收及产品 Watch 竞争复现（2026-09-09）

fork CI `34303780336` 已 completed/success：head
`832b70fd622f20d39a82ce6b50d3938a94b8bbd3`，test/security 均 success。
产品工作树 go.mod/go.sum 更新至 Go 实际解析的远端不可变版本
`v2.0.8-0.20260909023231-832b70fd622f`；下载模块五个改动文件逐个与 fork commit 比对
一致，`go mod verify` 通过。模块及 go.mod 摘要分别为
`h1:xmTt2n1e/Yy8Tq5cCn4MqQsTtzTQWMuKX2GAqDkJuJg=`、
`h1:V4mVPYUvt3A19cV1MoZtZdy3LrQ+xdzAnSbWXEz9wCI=`。不是本地 replace。

产品新增 `TestTiKVClientSafeTSUsesEncodedRegionRanges`，真实 NewRPCClient 结合产品
`newTiKVProtoCodec()` 经 loopback gRPC 验证 v1/v2 txn，重试两次并检查输入不变。
同一测试用仓库外旧依赖 modfile 得到 RED（两模式 expected 50 / actual 900），
固定新依赖 race 20 次 PASS（1.531s）。日志
`security-client-safets-application-before.log` 与 `...-application-targeted.log`。
完整 storage/backend/build 普通测试 PASS（0.218/51.377/0.397s），storage/backend race
PASS（1.665/72.882s）；日志 `...-application-full-{unit,race}.log`。全产品 build 和
storage/backend/server vet PASS。实际 cmd 与 rollout probe 依赖图 govulncheck v1.6.0
为 0 reachable、0 imported-package、1 module-only，不能称所有模块漏洞均为零；日志
`...-application-vulnerability.log`。这些结果尚不能代替此次依赖变更的生产前后门禁。

真实后端验证先按已有权威 Pod JSON 逐一检查 PD0/TiKV0..2 UID、Ready 和无删除时间戳，
再读 PD Region 清单。仓库外 `safets-region-readonly.go` 仅调用 GetStoreSafeTS RPC：
每个 Region/Store 先读 PD 原始 Region 边界参考值，再读校正后的逻辑范围请求，最后
复读参考值；仅在参考值稳定且非零时比较，变化则有界重试。5 个 Region（2002/2006/
2008/2010/2012）、3 个 voter Store（2001/2004/2005），15 组全部一致。旧逻辑范围
直传有 3 组与稳定参考值不同，本次均为更低水位，不是线上复现 50→900。
`security-client-safets-real-region-readonly.log` 最终为
`READONLY_SAFE_TS_CHECK_PASSED regions=5 stores=3 comparisons=15 legacy_differences=3 writes=0`。
Region 清单前后 count/ID/bounds/peers 投影一致；JSON 为
`security-client-safets-real-regions{,-after}.json`。全程没有数据写入、Region split、
后端重启或 Pod 创建；三个 loopback 转发已结束，3659/32001/14499 均无监听。
这不是新镜像在线升级验收，也不证明历史冷启动根因。

首次完整 server/probe 普通测试 FAIL：
`TestFollowerWatchRejectsInvalidProxyHeaderBeforeAuthoritativeCreate` 等待拒绝响应超时，
probe 包独立 PASS（185.324s），etcd 包 FAIL（143.267s），日志
`security-client-safets-server-probe-unit.log`。串行命令因该失败终止，后续 race 没有启动。
原 Watch 测试单项普通 20 次与 race 200 次复测均 PASS（0.296/10.094s），不撤销完整失败。

核对 `/root/etcd/server/etcdserver/api/v3rpc/watch.go`，创建失败必须发送
Created=true、Canceled=true、WatchId=-1 的响应。KubeBrain 的
`rejectAuthoritativeCreate` 先发布响应/关闭 ready，再取消 generation；`sendControls`
直接 select ready/cancel，二者同时关闭时可能随机选择取消并丢弃已发布的拒绝。
新增四状态回归每种预排队 64 个 control 后才启动 sender，覆盖已就绪拒绝+取消、未就绪
取消、已过期成功创建、正常成功创建，并核验完成通知和 barrier 全部释放。旧生产代码
立即 RED：`security-client-safets-watch-control-before.log`（0.042s，拒绝误返回 context canceled）。

修复取消分支先非阻塞复核 ready；只在确实未发布时丢弃，读取响应前保持 channel 同步。
过期成功创建仍由 closing/generation 检查抑制，不放宽响应顺序或取消隔离。
当前已启动修复后两用例 race 100 次及完整 server/probe 普通/race，日志
`security-client-safets-watch-control-fixed.log`、`security-client-safets-watch-fixed-full-{unit,race}.log`；
尚待终态，不登记完整 PASS。产品改动仍未提交或推送，没有触发新的镜像构建/部署。

修复后的两项 Watch 回归 race 100 次已 PASS（6.502s，原会话 95622 exit 0），
完整 server/probe 普通测试 PASS（etcd 142.957s、probe 180.338s，原会话 22892 exit 0）。
相关 vet 与全产品 build 复核 PASS。verbose 安全复查明确剩余 module-only 为
GO-2026-5932（x/crypto/openpgp 未维护），实际产品入口没有导入或调用；结果仍为
0 reachable、0 imported-package、1 module-only，日志
`security-client-safets-watch-vulnerability-verbose.log`，原会话 60273 exit 0。

五个产品 Go/模块文件冻结摘要为 `security-client-safets-watch-release-code.sha256`，
复核全部匹配。此次 pre verifier PASS（709，171/194/181/163），日志
`security-client-safets-watch-pre-verify.log`；四个并行分片日志
`security-client-safets-watch-pre-{0,1,2,3}.log`，原会话依次 76136/82494/73682/3273。
完整 server/probe race 原会话 18097，根模块除 `hack/production` 单包以外的全量普通
测试原会话 5612（`security-client-safets-watch-all-nonproduction-unit.log`）；后者只把
生产单包交给上述精确分片，未排除其它目录或用例。03:12 UTC 时这些检查仍运行，
先轮询原会话取得终态，不因观察超时重新启动；未产生 pre 全量完成回执，更无 post 结果。

只读重查实例仍是 StatefulSet UID `2650ad15-1d37-41c4-836c-d40dd4502720`，
3/3 Ready，current/update revision 均 `kubebrain-696c87f8f9`，镜像 digest
`a245c95fea36c387358d86e3808a9d29073a327028d5a4e3a80e4d272663e865`。此次未变更部署。
下一步在本轮所有前置测试成功后提交，立即执行同一 verifier/四分片 post，再正常
FF push 并核验新镜像 CI。在线升级需要新的目标镜像/runtime digests、独立客户端
TLS Secret 与 info CA/SAN；不得复用旧 `run-security-controlled-ha.sh` 冒充升级入口，
它固定旧证据并执行 leader 删除，不是此次候选镜像升级流程。

完整 server/probe race 已 exit 0（etcd 438.531s、probe 267.116s），除生产单包以外
的全部根模块普通测试也已 exit 0；不是仅跑新增 Watch 用例。全根模块
`govulncheck v1.6.0 ./...` 再检查 exit 0，仍为 0 reachable、0 imported-package、
1 module-only，日志 `security-client-safets-watch-all-vulnerability.log`。
只读真实后端门禁 exit 0：3 PD、3 TiKV、异常 Region=0、连续三次样本通过；同一门禁
也检查真实数据 PV 的 Retain 和消费者 CSI 身份。日志
`security-client-safets-watch-backend-readonly.log`。没有通过此检查调整存储或重启后端。

本轮 pre verifier 及四分片全部 exit 0：709 项，171/194/181/163，四片耗时分别
487.684/496.804/335.870/769.632s；五个冻结文件摘要再次全部匹配，`git diff --check`
通过。全根模块 `go vet ./...` 亦 exit 0，日志 `security-client-safets-watch-all-vet.log`。
此次提交包含远端客户端固定版本、产品线协议回归及 Watch 拒绝响应竞争修复，旧失败
日志保留。提交后立即执行相同 verifier/四分片，只有 post 全部成功才允许正常 FF push；
此处尚未声明 post 完成，也未运行新的镜像 CI 或在线升级。

产品提交为 `03480da7e168c6888e11ac51dfc4bd46ad88d07a`，提交后工作树干净。
立即执行 post verifier 已 PASS（709，171/194/181/163），四片已并行启动：
`security-client-safets-watch-post-{0,1,2,3}.log`，原会话分别
60483/83867/88336/44673；verifier 为 `security-client-safets-watch-post-verify.log`。
先轮询这四个原会话，只有终态全部 exit 0 且冻结摘要匹配后才登记完成、快进推送。
本段只登记提交后复验入口，不改变产品源码，不把正在运行写成已经通过。

等待 post 期间补充纯离线冷启动归因核查：使用实际部署的 keyspace
`kubebrain-dbaas-test`、默认协调前缀 `/kubebrain-internal`，由当前产品 coder 和
远端 API v1 txn codec 生成 object/durable/compact 三个范围，再与已采集的 5 Region
元数据按半开区间交集比较。三者旧 raw 请求与修复后编码请求都仅命中 Region 2006。
日志 `security-client-safets-checkpoint-region-offline.log`，程序
`checkpoint-region-offline.go` 均在仓库外测试状态目录；exit 0，网络调用/数据写入均为 0。

这收窄了归因：至少在此次采集的拓扑中，编码变化不会让本实例这三个 checkpoint 范围
选择不同 Region。仍需保留通用编码修复，以应对其它键范围和后续 Region 分裂；但不能
因此宣称早前 10m26s 首次初始化缺口已关闭。没有取得当时各 Region/Store 的水位、锁和
拓扑完整时序，不将当前离线映射扩大为对历史根因的证明，也不降低 checkpoint 安全要求。

随后只读复核三个当前源副本的 HTTPS `/capabilities`：按 HA 后权威 Pod 清单逐个校验
UID/Ready，直接使用 immutable `kubebrain-test-info-ca` ConfigMap 的公开 CA 和
`kubebrain-peer.kubebrain-dbaas-test.svc` SAN，不带任何客户端私钥。三个副本均提供
`snapshot-history-pin-before-write-barrier-release.v1`，包括 HA 后新 UID
`334eae03-1f53-4eb5-93c7-e230bec2b50b` 的 Pod 2。前后 Pod UID 一致，临时 loopback
转发均停止，18880 已无监听。脚本 `verify-watch-release-info-readonly.sh` 与日志
`security-client-safets-watch-info-readonly.log` 位于仓库外，原会话 46107 exit 0。
未执行旧硬编码 Pod 2 UID 的验证脚本，未覆盖旧证据，也没有升级、重启或集群对象写入。

`03480da7` post verifier/四分片均已 exit 0：709 项，171/194/181/163，耗时依次
463.353/473.811/306.596/753.177s。五个冻结文件摘要全部再次匹配，终态确认后才创建
`security-client-safets-watch-post-complete.json`。本轮提交前后完整门禁均通过，之前
服务端拒绝响应失败及客户端随机测试失败仍保留为历史证据，不用单项重试替代完整结果。

推送前远端 dbaas 仍为 `60137eb36ee7e4a8f61251a6f91b36ae1135aa23`，最近镜像 run
`34290666105` 已 completed/success，没有运行中的镜像构建。现在准备正常 FF push
包含 `03480da7` 的完整分支，并核对随后 CI 的精确 head SHA、最终状态和不可变镜像。
当前记录只允许进入构建阶段，不代表镜像已发布或实例已升级。后续不得把旧 `60137eb3`
镜像用作这次修复的部署证据。

正常快进推送已完成：`60137eb3` → `de8a9e1f2ce9f5c0a0de802a98d79af871a12e20`，
包含产品提交 `03480da7`；推送后本地与 origin/dbaas 一致。自动触发镜像
[CI 34307884732](https://github.com/fivetime/kubebrain/actions/runs/34307884732)，
head SHA 精确为 `de8a9e1f...`，self-hosted job `102328262921` 已 in_progress，当前
`Scan shipped Go modules`。状态快照为 `security-client-safets-watch-image-ci.json`。
后续轮询此 run，而不是再次推送/dispatch 取消它；尚无本轮镜像 digest 或 CI success。
本段在推送后追加，暂不再推送。当前服务镜像仍未改变，在线升级必须等这次精确源提交
的完整 CI/镜像验证成功后执行。

CI 后续已通过 `Scan shipped Go modules`、双架构 kubectl 构建/扫描及前置架构检查，
进入 `Build and push TiKV test image`，仍 in_progress。未再次推送或 dispatch。
为本次候选镜像单独准备仓库外 `run-security-watch-client-upgrade.sh`：默认 `verify`
只读，只有显式 `execute` 才进入现有 candidate rollout runner；固定本次 CI/head SHA、
709 项 post 回执、运行中旧镜像、namespace/StatefulSet/TidbCluster/StorageClass UID。
它还要求后端仍为 PD/TiKV v8.5.3、各三副本，真实卷通过 Retain/消费者 CSI 门禁，
独立 client TLS Secret 与 info CA ConfigMap 身份吻合。运行源码必须与 `de8a9e1f`
一致（只允许文档差异），镜像 index 和两个平台 manifest digest 必须与验收回执一致。

执行入口及显式 kubeconfig 的两个辅助脚本摘要固定在
`security-de8a9e1f-upgrade-tools.sha256`。`bash -n` 通过；没有实际镜像回执时，默认
verify 在访问 GitHub/registry/集群前退出 1，日志
`security-de8a9e1f-upgrade-no-image-version-guard.log` 明确拒绝。
这个负向检查不是完整升级预检成功。当前不存在、也未伪造候选镜像回执。

CI 终态 success 且镜像实际验收通过后，才创建
`security-de8a9e1f-image-verified.json`，字段遵循既有镜像回执：run_id=34307884732、
source_commit=de8a9e1f2ce9f5c0a0de802a98d79af871a12e20、image（不可变 index 引用）、
amd64_digest、arm64_digest、conclusion=success 及 evidence。之后先运行新入口 verify，
再执行 execute；保持 900 次操作、5s public/30s direct SLO，不附加 leader 删除、后端
重启或网络故障。回滚、UID/RV fencing、夹具清理和运行时 digest 收敛由现有已测试
runner 负责，仍须读取真实终态，不能将准备入口等同于部署完成。

为避免重复 dispatch 或以观察超时误判停止，已启动
`gh run watch 34307884732 --repo fivetime/kubebrain --interval 45 --exit-status`，原会话
47228，日志 `security-de8a9e1f-image-watch.log`。Runner 为 `raas-1519`，标签
`self-hosted`；构建步骤自 03:43:34 UTC 起执行。03:53 UTC 再查仍为 in_progress，
没有终态失败声明。后续先轮询这个 watcher，会话结束后再从 GitHub 独立核验 run/head
与完整日志；watcher 的连接错误本身不等于 CI 失败。镜像回执仍未创建，未运行 execute。

### 本轮镜像验收完成，进入候选升级预检（2026-09-09）

watcher 47228 已 exit 0，并独立查询 GitHub 确认 run `34307884732` completed/success，
head 精确为 `de8a9e1f2ce9f5c0a0de802a98d79af871a12e20`。job 从 03:38:36 到
04:10:33 UTC，31m57s；`Verify published test image` 和 `Promote verified image to dbaas`
均 success。完整终态/日志为 `security-de8a9e1f-image-ci-completed.{json,log}`。

本轮 OCI index：`sha256:0044f89deef94bc6cebae6e548f3715152a35a5b83e63ed895575eccd3dc28c6`；
amd64：`sha256:a83111ee126cb402a0bbe44a3944a04b441c7c9f15c45973b9902fc62861de0f`；
arm64：`sha256:50f47075a56daf1705992953bac7bba45dd6efcf1667cf84cc0dd1e2dce2e767`。
按 index digest 复读的 manifest 与本轮 immutable tag 完全一致，平台选择器验证通过，
`dbaas` 标签也指向同一 index。证据 `security-de8a9e1f-image-manifest-by-digest.json`、
`security-de8a9e1f-promoted-image.json`；初次构建中 tag 未发布和 job log BlobNotFound
仅为当时观察结果，不是终态失败。

另外拉取 amd64 成品，复制实际 kube-brain 二进制并用 `go version -m` 核对：Go1.26.8，
上游模块 v2.0.7 replace 为 `v2.0.8-0.20260909023231-832b70fd622f`，模块 h1 与 go.sum
精确一致。断网、只读、去除 capabilities 的临时容器执行 version，Git SHA/版本与
OCI labels 一致；日志 `security-de8a9e1f-image-main-build-info.log`、`...-image-version.log`、
`...-image-labels.json`。首次包含强制删除的组合命令被本机执行策略拒绝，未执行；随后
分步完成实际审计。临时容器 bdf93afd... 和目录 image-de8-module.DPdeYQ 中的复制二进制
已按明确对象非强制删除，共享镜像缓存未清理；可从不可变镜像重新取得成品。

registry cache 首轮导入标签尚不存在，保留既定 GHA 只读回退；本轮 cache export
`#172 DONE 481.1s`，相比此前 GHA export 3606.3s 明显缩短，不能把单次结果当作长期
性能保证。远端 post Buildx cleanup 对 builder-01f79580-7e70-4195-b443-d2374855a57e
state volume 删除超时报 warning，整体 job 仍 success；未声称远端残留已清理，也未对
Runner 主机执行猜测性的 prune/删除。这不改变已经通过的镜像验证结果。

上述证据齐备后才创建 `security-de8a9e1f-image-verified.json`，并保留 cleanup warning。
现在进入新入口的 verify；此处还没有升级或部署成功声明。产品源码仍与发布提交一致，
仅此测试文档存在未推送追加，避免再次触发无关镜像构建。

新入口 verify 已 exit 0（原会话 23869），CI/index/平台 digest、源实例身份和后端
门禁全部通过，明确 `cluster_mutations=0`。执行前进一步核对发现默认 900×0.1s 的
前台操作只保证约 90 秒，可能先于允许 300 秒的三副本滚动结束。为本次真实验收把
仓库外 launcher 的 PROBE_ITERATIONS 提高到 3600，明确 ROLLOUT_TIMEOUT=300s；
探针循环逐次 `time.Sleep(100ms)`，因此操作至少持续 360 秒，覆盖完整滚动预算。
保留 5s public/30s direct SLO 和 lease TTL=5，不放宽延迟阈值或修改产品运行代码。
这是对本次测试覆盖范围的补强，不宣称默认 runner 已有持续到 rollout 完成的握手。
后续应补默认 runner 的覆盖保证，不能对任意耗时滚动只凭 900 次 summary 宣称全程可用。

已重新冻结 launcher/辅助工具摘要、完成 `bash -n`，3600 次配置的只读 verify 再次
exit 0（原会话 81489），日志 `security-de8a9e1f-upgrade-3600-verify.log`。
准备以此入口 execute，日志 `security-de8a9e1f-upgrade-execute.log`；此刻尚未记录
升级完成。没有叠加 HARD_FAILOVER、后端重启、节点或网络故障。

execute 已启动，原会话 61329；后续先轮询这一会话及
`security-de8a9e1f-upgrade-execute.log`，不要另起第二次 execute。入口重新确认源码、
辅助脚本、CI、镜像和后端门禁通过后才进入 runner；首次只读观察时探针 Pod 尚未创建，
会话仍运行，这不是终态失败。只有 runner 的真实终态、候选 Pod digest/revision 收敛、
3600 次完整 summary 和夹具清理证据均确认后，才能登记本次在线升级通过。

### 候选升级首次真实失败：冷镜像拉取超出直连恢复预算（2026-09-09）

本轮已真实进入候选滚动，不是只做预检。探针 Pod UID
`5847a191-42b2-460e-b38e-3a68d0dc1070` 在 k8s3-network2 于 04:20:06 启动，
04:21:17 exit 1：`PROBE_FAIL iteration=110: direct watch recovery exceeded 29.767303589s: context deadline exceeded`。
没有 3600 项完成 summary，不能登记可用性 PASS。完整失败日志已在自动清理前保存为
`security-de8a9e1f-upgrade-probe-failed.log`，Pod/events 现场分别为
`security-de8a9e1f-upgrade-{pods,events}-failure.json`。

首个候选 Pod 2 UID `ec8793a9-ceb1-4372-a395-017291c2c052`：04:20:57 创建，
04:20:58 调度到 k8s3-worker2，两个临时消费者卷同秒完成 attach；初始存在旧 ephemeral
PVC owner/删除等待事件。04:21:06 开始拉取候选镜像，04:21:43 完成，kubelet 明确记录
36.973s、2011990283 bytes；随后启动，04:21:50 Ready。仅镜像拉取就超过 30 秒直连
恢复预算，因此不能靠下一次恰好已有热缓存就称冷镜像升级已通过；后续应补目标节点的
候选镜像预拉取保障，保留原 SLO。此证据解释停机窗口的明确组成，不声称排除了所有
DNS/连接恢复等其它影响，也不是先前 10m26s checkpoint 初始化的根因证明。

runner 当前先等 StatefulSet rollout，再检查 probe 终态；本次 probe 已失败后仍继续
滚动剩余副本，随后才进入现有回滚。这暴露需要补强的 fail-fast 监控，不能将仅有最终
回滚等同于及时停止扩散。本轮没有手动 patch 与活跃 runner 竞争，也没有编辑正在执行
的产品脚本。04:26 UTC 已观察到 `availability probe failed` 和 `restoring original image`，
StatefulSet desired 已回到旧 index a245c95f...，current revision 仍是候选
kubebrain-8d5549fff，update 为旧 kubebrain-696c87f8f9；回滚尚未完成。
原会话 61329 仍运行，继续跟踪回滚与 fixture 清理，不重复 execute。

另一个待核对的运行时兼容边界：本集群 CRI-O 的候选 Pod status.imageID 报告的是
已验证的 OCI index digest 0044f89d...，而本次 launcher 的 TARGET_RUNTIME_DIGESTS
目前只列两个平台 manifest digest。本轮在 probe 阶段就失败，尚未执行最终 runtime
校验，不能将该差异冒充本次失败原因；后续应在固定 index/架构 manifest 证据下适配
CRI-O 的报告形式，不能为通过检查接受任意 digest。

原执行会话 61329 已 exit 1，失败结果保留；随后独立读取确认回滚已完成：StatefulSet
UID 未变，generation/observedGeneration 均 3，current/update revision 均
`kubebrain-696c87f8f9`，3/3 Ready，完整 spec 与执行前 source JSON 相等，三个 runtime
imageID 均回到旧 a245c95f...。回滚后的 Pod UID：0=a621367f-0f46-42f7-b9d8-74a2dfafe229，
1=f76014ff-d2b1-4ad4-a697-e308819a27fc，2=bb166062-1064-417d-9fdd-a4a56e77eadf。
原 HA 后的 frontend UID 清单已过时，后续只读验证应以本次 final-pods 为新的定位依据。

最终日志确认 `FIXTURE_CLEANUP_OK status=absent owner_uid= keys=0 users=0 roles=0 leases=0`，
独立查询所有以 security-upgrade-de8a9e1f 开头的 Pod/ConfigMap 均不存在。
`security-de8a9e1f-upgrade-final-{statefulset,pods}.json` 保存回滚终态。再次执行真实后端
门禁 exit 0：3 PD、3 TiKV、连续三次异常 Region=0、数据卷 Retain/消费者 CSI 通过，
日志 `security-de8a9e1f-upgrade-final-backend.log`；六个后端 Pod UID 和 restartCount
与之前权威清单一致，未由此次测试重启。测试残留对象已清理，日志可审计，当前没有
运行中的升级会话。本轮结果是“镜像 CI 成功、真实升级失败且已回滚”，不是部署成功。

下一步应先实现并验证升级前目标节点镜像预拉取、滚动期间 probe 失败的及时回滚及
覆盖整个滚动窗口的探针生命周期，再核对 CRI-O index 报告的受限接受条件。不得只因
三个节点经过此次失败已缓存镜像就直接重试并关闭冷镜像升级缺口。

### 2026-09-09 滚动期间探针监控修复（本地验证，未部署）

前一轮升级和回滚均已终态，本轮只修改本地 runner 与其测试，未再次执行集群升级，也未创建上游 PR。
修复前的 `security-rollout-observer-before.log` 记录四个 RED 场景：失败/提前完成/被替换的探针无法及时
终止长 rollout 等待；rollout 边界已 Succeeded 的探针还可能错误放行。原测试会话 84628 已不存在，
日志明确为 FAIL，不据此重复启动真实升级。

新实现让有界 rollout 观察进程与 UID 约束的 probe Running 检查并行，观察结束后再次确认原探针仍运行。
失败路径先回收观察进程，再进入既有 spec/UID/resourceVersion 约束的回滚；诊断日志留在回滚处理之后读取，
并隔离超限/读取失败，保证补偿清理仍执行。正常延迟 rollout、异常观察进程退出及原超时/替换/回滚测试
已通过：`security-rollout-observer-targeted.log` exit 0，91.405s。补齐后的九个异常子场景包括 Pending、
缺失 phase、卡住的 probe API、不可读/超大失败日志，`security-rollout-observer-final-targeted.log`
exit 0，54.562s；同时验证恢复原镜像、观察进程不存活、无覆盖通过标记，诊断异常仍删除自有 fixture。
`bash -n`、`git diff --check` 和 `go vet ./hack/production` 均通过；这些不是实际数据面升级成功证据。

产品源码已冻结，校验清单为私有审计目录的 `security-rollout-observer-code.sha256`。提交前完整门禁已启动：
`hack/production/test-shard.sh --verify 4` exit 0，710 tests，四片 171/194/182/163；日志
`security-rollout-observer-pre-verify.log`。四片 0/1/2/3 原会话分别为 71910/72953/99396/49331，
日志 `security-rollout-observer-pre-{0,1,2,3}.log`；此条记录时仍在运行，尚未记为通过、尚未提交产品代码。
后续必须读取这四个会话的终态；源码未变且全部通过后才可提交，产品提交后仍须立即重跑 verifier 与四片。
真实升级失败、冷镜像 36.973s 拉取及 CRI-O index digest 的未闭环状态不因本轮本地修复而改变。

上述首轮完整门禁不能记为通过：片 2 exit 1（364.348s），`TestValidateTiKVRegionHealth` 的磁盘压力
案例没有输出预期诊断；片 0 exit 1（476.179s），数据卷 retention 两个子案例分别提前报 TiKV PV/PD PVC
读取失败。片 1 exit 0（495.447s），片 3 此条记录时仍在运行。独立复查原健康检查用例 exit 0（95.297s），
`security-rollout-observer-region-health-recheck.log`，但单次复查通过不覆盖首轮完整门禁失败。

检查发现该测试的普通 fixture 将每次查询限制为 1 秒，而生产默认为 10 秒；本机当时 load average
13.11/12.23/7.83，内存和临时盘有余量。由于原失败未保留具体退出码，只能把调度/查询超时视为待确认因素，
不能断言所有失败都是负载造成。确定的问题是 `df` 查询的命令替换在 `set -e` 下会无诊断退出：新增
PD/TiKV 各两个明确注入的失败/超时案例，在原脚本上全部 RED（8.603s），输出均为空，日志
`security-rollout-observer-disk-read-before.log`。后续补诊断并重跑整套门禁，不能只重跑失败断言后提交。

预拉取准备仅做了只读检查，未创建 Pod：三副本 UID 与回滚终态一致，源 StatefulSet 的 required node
affinity 将 hostname 限制为 k8s3-worker1/2/3，分别有一个副本，容器 imagePullPolicy=IfNotPresent。
三节点均 linux/amd64、kubelet v1.36.0、CRI-O 1.35.3，Ready=True、DiskPressure=False，无 taint/cordon。
节点 UID 分别为 215fb73f-6e5c-4bf5-b9e4-63411fdccee0、9658dbb9-583f-40f3-b7e8-9a33508649fb、
1b181915-819a-43d0-88e8-8d232c67a1f7；完整只读证据为 `security-rollout-prewarm-{pods,nodes}-readonly.json`。
源模板没有 imagePullSecrets/runtimeClassName；未来通用预拉取实现仍须考虑这两项，不能只按本环境空值处理。

设计约束：预拉取应在业务 Pod 替换前完成，覆盖候选可调度节点，而非仅碰巧当前有副本的节点；绑定 Node UID、
平台、候选 digest 与临时 Pod UID，失败不得触发 StatefulSet mutation，部分创建后也须身份约束地清理。
临时 Pod 不需要业务/服务端 TLS、数据卷或 ServiceAccount token，应采用非 root、只读文件系统、drop ALL
capabilities，并使用正常调度约束。Kubernetes 文档指出直接设置 nodeName 会绕过 scheduler，且自动替换节点
会影响预拉取可靠性；缓存不能替代候选调度范围与身份复查，也不能绕过私有镜像凭证校验。
参考 [镜像预拉取与凭证校验](https://kubernetes.io/docs/concepts/containers/images/#pre-pulled-images)、
[节点分配与 nodeName](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#nodename)。
这部分是基于现场约束的实现准备，不是预拉取已完成或冷启动验收通过的声明。

首轮片 3 已 exit 0（772.141s），四片全部终态；首轮完整门禁的最终结论仍是失败，禁止用两片绿色替代。
确认最后一片结束后才编辑健康检查脚本，避免改变运行中测试所执行的脚本。现在磁盘查询失败会明确报告
组件、Pod 和命令退出码；生产 `PROBE_TIMEOUT` 默认仍为 10 秒，磁盘压力/容量/Retain 等验收条件未放宽。
普通语义 fixture 改用与生产相同的 10 秒查询预算，注入失败/卡住命令的专门测试显式使用 1 秒，外层仍限 10 秒。
四个明确注入的场景连续三次通过（25.888s），`security-rollout-observer-disk-read-after.log` exit 0；
分别锁定 exit=17 和 timeout exit=124 的诊断，不能误报 health gate passed。原首轮失败的具体退出码无法追溯，
该诊断改动不应被描述为已证明原失败全部由超时导致。

05:13 UTC 已冻结本轮四个产品/测试文件，清单 `security-rollout-observer-v2-code.sha256`，`go vet`、
两个 shell 的 `bash -n`、`git diff --check` 均通过。新 verifier exit 0：711 tests，四片 171/194/183/163，
日志 `security-rollout-observer-v2-pre-verify.log`。重新启动的片 0/1/2/3 原会话为
76262/30907/82623/12163，日志 `security-rollout-observer-v2-pre-{0,1,2,3}.log`；当前均运行中，尚无完整
通过回执、没有产品提交或 push。继续时优先读取这些原会话及对应终态，不以首轮日志代替新源码的门禁。
只有本轮完整门禁全绿且源码清单仍匹配后，才可提交；产品提交后立即再跑 verifier 和四片完整测试。

05:26 UTC，本轮提交前 verifier 与四片已全部通过，原会话均读到 exit 0。片 0/1/2/3 分别为
471.885/509.999/396.804/781.826s，覆盖完整 711 tests（171/194/183/163）；四个文件的冻结校验和再次全部
匹配。额外的进程组工具 race 测试 exit 0（3.145s），日志 `security-rollout-observer-v2-processgroup-race.log`。
首次 710 项门禁失败记录继续保留，不用新绿色结果覆盖历史失败或声称已追溯其所有具体退出码。
这组结果允许提交本地修复，但不代表候选镜像已部署，更不代表冷镜像/首次 checkpoint 初始化缺口已关闭。

另以 hostname label selector 查询整个候选节点集合，而非仅按预期名称读取：结果精确为已记录的三个 Node UID，
没有额外匹配节点，证据 `security-rollout-prewarm-pool-readonly.json`。未来执行前仍须重新验证集合及 UID，
不能长期复用本次只读快照作为节点预拉取完成凭证。

本地产品提交已完成：`d85a68812e8d2a2746010da630c18966110fabc2`
（`production: abort rollouts when availability coverage fails`）。提交后立即执行 verifier，exit 0，仍为
711 tests / 171/194/183/163；日志 `security-rollout-observer-v2-post-verify.log`。紧接着启动提交后四片，
片 0/1/2/3 原会话为 2494/33818/60943/28698，日志 `security-rollout-observer-v2-post-{0,1,2,3}.log`。
此条记录时四片均运行中，没有完整提交后通过回执，不能把提交前通过当作提交后通过。继续时保持产品源码冻结，
读取上述原会话终态并校验 `security-rollout-observer-v2-code.sha256`，不要重复启动同一轮测试。
本次没有 push、触发镜像 CI 或执行集群升级；冷镜像预拉取、CRI-O digest 约束及真实升级验收仍待后续实施。

05:36 UTC，提交后片 0/1/2 已 exit 0，分别 473.170/487.222/385.151s；最后片 3 原会话 28698 仍在运行。
以下只是等待期间的实现设计，没有修改冻结产品代码、没有创建集群对象：

- 预拉取临时对象不得复制业务 Pod 的标签集合，否则可能匹配 client/headless Service 的 selector。需对实际
  Service selector 检查，而非仅假定标签名称；临时进程不监听业务端口，不挂载业务证书、数据卷或 API token。
- 单次 version Job 完成不能证明缓存持续可用。kubelet 会回收未使用镜像，因此拟由候选镜像的 init container
  运行 `/usr/local/bin/kube-brain --version`，随后由同镜像的有期限等待容器保持使用；验证 init exit=0、实际
  Node/Job/Pod UID、Running/Ready、runtime imageID 和已批准的平台。可执行路径已从实际 Dockerfile 核对，
  不是 `/kubebrain`。依据 [镜像垃圾回收说明](https://kubernetes.io/docs/concepts/architecture/garbage-collection/#containers-images)。
- 拟用 batch/v1 Job 的 activeDeadlineSeconds 限制最长活动时间，并设 ttlSecondsAfterFinished 作为异常退出后的
  补充回收机制；正常路径仍主动执行 UID/resourceVersion 约束的删除并确认子 Pod 消失。TTL 不是即时清理证明，
  也不能替代活动期限或最终状态查询。现有 `uid-delete` 已支持 batch/v1/jobs 与 Foreground propagation，无需
  放宽其删除前置条件；它的 grace-period override 仍只允许 core/v1 Pods。依据
  [Job 活动期限](https://kubernetes.io/docs/concepts/workloads/controllers/job/#job-termination-and-cleanup)及
  [已结束 Job 的 TTL 清理](https://kubernetes.io/docs/concepts/workloads/controllers/ttlafterfinished/)。
- 准备阶段须在 probe 的业务计数窗口之前完成，不能让冷镜像拉取消耗已启动探针的整个生命周期。正式 mutation 前
  再验证源 StatefulSet spec/UID、完整可调度节点集合、Node UID、已预热对象身份及剩余活动预算；任一证据缺失、
  超时、过期或身份漂移都必须在替换业务 Pod 前失败。准备阶段部分创建失败必须补偿清理已确认自有的 Job。
- 通用实现需保留源 imagePullSecrets、ServiceAccount 与 RuntimeClass 的拉取环境，但禁用 token 自动挂载，
  不读取或复制 Secret 内容；必须测试源调度约束、多架构映射、创建响应丢失、对象替换、Job 提前结束、清理失败、
  Service 误匹配和冷拉取失败。已有热缓存不能代替这些故障路径及最终真实升级的验证。

05:41 UTC，提交后最后片 3 原会话 28698 已 exit 0（764.310s），完整 711 项提交后门禁全部通过。
四片 0/1/2/3 用时为 473.170/487.222/385.151/764.310s；verifier、四片终态、四个源码校验和及与
产品提交 d85a6881 的非文档 diff 均已核对。审计回执为 `security-rollout-observer-v2-post-complete.json`，
明确限定为本地产品验证，不是镜像发布、预拉取完成或真实升级验收。提交前后测试会话均已终态，没有需要继续
等待的旧会话；后续可以开始新的预拉取实现，但新修改不能沿用这份回执作为其完整测试证明。

### 预拉取 Job 生成组件草稿（未接入 runner，未提交产品）

完整提交后门禁结束后，在 `hack/production/internal/imageprepull` 新增纯 Go Job 生成器及测试，不调用 API、
不创建 Kubernetes 对象。生成器绑定源 StatefulSet UID/spec digest 与目标 Node UID，要求不可变候选镜像、
Linux amd64/arm64 标签与 NodeInfo 一致、Ready 且无 DiskPressure。要求 client/headless Service 都在同一
命名空间的输入清单内，并拒绝任何清单内 Service 会选中预拉取 Pod 的情况；完整清单获取仍是后续执行器责任。

输出为一个 init version 容器及一个同镜像的定时等待容器，带活动期限、TTL、零重试、最小资源请求/限制，
非 root、read-only rootfs、drop ALL、RuntimeDefault seccomp、禁用 API token；不复制业务环境变量、端口、
卷和证书。保留源 imagePullSecrets、ServiceAccount、RuntimeClass、scheduler、nodeSelector、tolerations 及
node affinity，并在 required affinity 的每个有效 OR 分支中增加精确 metadata.name 条件，保持正常调度。
业务 Pod anti-affinity 不复制到 holder，以允许它与服务 Pod 共处同一节点。

初稿测试发现并修正了自定义 scheduler 丢失、源固定 nodeName 不一致未拒绝、空 required affinity 分支被
错误变为有效分支三项问题；RED 日志为 `security-image-prepull-builder-before.log`。后续测试覆盖全部六个
label 操作符、字段约束、空/非法分支、超过 float64 精确范围的 int64 比较、平台/期限边界、对象删除状态、
Service 误匹配及输入深拷贝隔离。最终 `go test -race ./hack/production/internal/imageprepull -count=3 -cover`
exit 0（1.155s），语句覆盖率 98.9%，日志 `security-image-prepull-builder-final.log`；`go vet`、gofmt 与
diff 检查通过。两个新文件的校验清单为 `security-image-prepull-builder-code.sha256`。

当前两个新文件尚未提交，runner 未引用该包，也没有 CLI 或实际预拉取回执。下一步仍需实现完整候选节点清单、
创建/观察/补偿清理、实际 Job/Pod/runtime 身份校验和升级脚本接入；再跑相关及完整门禁，并完成真实升级验收。
不要将此处纯生成器单测、98.9% 覆盖率或先前 d85a6881 的 711 项回执冒充整条预拉取流程已通过。

### 预拉取执行器与完整硬调度节点池（2026-09-09，仍未接入 runner）

随后继续实现同一包；当前为七个未提交文件，不再是上述两个文件的纯生成器版本。先前生成器校验清单和
98.9% 覆盖率只属于当时的源码，不适用于本节版本。没有修改既有 rollout/health 脚本，没有 push、触发 CI、
创建真实集群 Job 或替换业务 Pod。

执行器现在支持先核对全部目标名称、逐一创建、轮询就绪、升级前再验证和有期限补偿清理。准备失败时使用
独立清理上下文，取消准备不能自动取消清理。CREATE 响应丢失时，仅通过随机 attempt 标记和精确源 owner
识别该次尝试；成功 CREATE 返回的具体 UID 在策略校验之前记录，因此 admission 修改 owner/标记导致拒绝
时也不会遗漏自有对象。清理仅删除已确认 UID，使用 UID/resourceVersion 前置条件与 Foreground，冲突后
重新读取 RV，且必须确认 Job 及其自有 Pod 都消失；DELETE 被接受、活动期限和 TTL 都不能代替最终清理证据。

实际 Job/Pod 验证覆盖身份、策略、Service selector 隔离、init version 成功、holder Running/Ready、容器
重启/失败、目标节点与经批准的平台 imageID。支持 CRI-O 等明确身份形式，但不会仅凭任意字符串的 digest
后缀接受镜像。最后证据读取后再次检查取消状态及最早 holder 的剩余生命周期，避免读取其他节点期间
消耗完前面节点的活动预算。源 StatefulSet 必须在单一稳定 revision、完整 Ready、generation 已被观察，
且 UID/spec 未改变。生成的 Pod 额外禁用 ServiceLinks 和抢占。

本轮补齐 `DiscoverPlacement` 和执行器内节点池复核：

- 对所有 Node 做不带 label/field selector 的 LIST，Limit 257；有 continuation 或超过 256 项即拒绝，
  不把截断列表当完整证据。名称/UID 必须有效且唯一，硬条件匹配的目标总数限定 1..32。
- 按源 nodeName、nodeSelector、required node affinity 与 RuntimeClass 的 nodeSelector 交集确定池。
  不按当前业务 Pod 位置、暂时资源余量、软约束、Ready、cordon 或 taint 缩小池；这些可变条件可能在升级
  期间改变。池内节点必须健康并真正运行 holder 才能继续。因此不可容忍污点或自定义 scheduler 可以阻止
  准备完成，但不能静默把相关节点排除后声称覆盖完整。此处是保守硬调度覆盖，不是完整 scheduler 仿真。
- RuntimeClass 必须有正确名称、UID/RV、handler 且未删除；合并 nodeSelector（冲突拒绝）、toleration
  并集/冗余消除和固定 overhead，保留源快照不变。overhead 冲突按资源数量语义比较。暂不猜测 Gt/Lt
  toleration 的集群 feature gate，遇到这类 RuntimeClass 合并输入明确拒绝。实现依据
  [Kubernetes v1.36.2 RuntimeClass admission](https://raw.githubusercontent.com/kubernetes/kubernetes/v1.36.2/plugin/pkg/admission/runtimeclass/admission.go)
  及对应 toleration union 行为；尚未经过真实 apiserver admission 的端到端验证。
- 创建任何 Job 前、各次创建前、观察开始和最终证据边界均复核完整池及 RuntimeClass。新增/遗漏/替换
  Node、平台/runtime 变化、RuntimeClass 实质变化均拒绝；仅 RuntimeClass RV/标签更新不误判为策略漂移。
  这不构成后续业务镜像修改的原子围栏，runner 接入时仍须立即复核并对源修改使用 UID/RV/spec 约束。

测试证据均在仓库外既定审计目录：

- `security-image-prepull-executor-ownership-before.log` 记录 CREATE 成功但 admission 改 owner/标记时
  两项漏清理 RED；`security-image-prepull-executor-cancel-before.log` 记录最终读取取消仍返回成功的 RED。
  修复后的旧版执行器日志 `security-image-prepull-executor-verified.log` 为 race count=3 通过，4.061s，
  87.6%；本次开始已核对该日志，并确认没有遗留测试进程，没有重启旧会话。
- `security-image-prepull-placement-before.log` 在测试编译问题修正后，记录缺少 RuntimeClass 合并/
  身份检查、遗漏节点仍成功、最终 Pod 读取中节点池扩大仍成功等真实断言失败。原会话 45759 exit 1。
- 初次全包 race `security-image-prepull-placement-final.log` exit 1：30ms 清理期限到达时，错误既可能
  从等待点也可能从下一循环入口返回，原测试只接受某一文案。已改为检查 `context.DeadlineExceeded`、
  恰好一次已接受删除及仍存在的依赖 Pod；没有放宽清理期限或以删除请求成功代替清理成功。
- 最终 `go test -race ./hack/production/internal/imageprepull -count=3 -cover` exit 0，4.926s，语句
  覆盖率 89.2%；日志 `security-image-prepull-placement-verified.log`，原会话 74028 已终态。覆盖
  RuntimeClass/Node 身份漂移、创建间池扩大并补偿、完整清单边界/错误/取消、硬 affinity 空分支、
  RuntimeClass 合并/深拷贝、手工给定的 admission 后策略及真实 JSON 往返序列化。`go vet` 和 diff
  检查通过，七个文件校验清单 `security-image-prepull-placement-code.sha256` 已逐项验证。

仍未完成且不能据此宣布升级可用：API transport 请求时间/响应字节上限、OCI index 与每个平台 digest
的独立发布证据校验、变更前持久化 attempt/所有权回执和进程崩溃恢复、CLI/runner 接入、真实 admission
验证、全窗口探针预算及新的完整提交前后门禁/镜像 CI/真实升级验收。Session 当前仍仅在内存中；失败路径
返回 nil，即使补偿失败也没有持久化恢复凭证，不能把 deadline/TTL 当作解决该缺口。源码仍未提交；此包的
单测也不属于既有 711 项顶层分片清单，后续产品提交必须同时执行本包测试和完整规定门禁。

### 预拉取清理回执与进程状态丢失后的恢复（2026-09-09，未提交/未部署）

本轮从七文件版本校验和全部匹配、无旧测试进程的状态继续，新增 `recovery.go` / `recovery_test.go`，
并修改执行器接入；目前为九个未提交文件。上一节列出的“仅内存清理记录”缺口已有以下组件级实现，但
CLI/runner 仍未接入，不能把它视为部署恢复能力已经交付。

- `RecoveryJournal` 是清理专用回执，只记录版本、Namespace 名称/UID、源 StatefulSet 名称/UID、随机
  attempt、各 Job 名称、创建意图、已确认 UID 和已确认清理状态。不保存 kubeconfig、密码、Secret
  内容、业务 env/卷配置，亦不保存可直接授权升级的准备成功状态。
- 调用方提供当前用户拥有的 0700 私有本地目录，回执和稳定锁文件均为 0600。创建拒绝覆盖已有回执；
  使用非阻塞独占 flock，更新采用同目录独有临时文件、文件 Sync、rename、目录 Sync。关闭只释放锁，
  保留回执和锁文件。依赖本地文件系统的持久化语义，不宣称已验证 NFS 或真实断电恢复。
- 加载限制 64 KiB，拒绝未知字段、重复字段/非规范 JSON、截断、非法状态/身份、软/硬链接、特殊文件及
  不安全权限。源码核对与测试发现 `os.Root` 会解析目录内软链接，因此不能仅依赖路径不逃逸保证；现在
  在打开前 Lstat 拒绝非普通文件，并核对打开后的身份。固定锁不随 JSON 文件替换而更换。
- `Prepare` 在任何 CREATE 前持久化该目标的意图，成功返回具体 UID 后立即持久化 UID，再检查 admission
  策略。已绑定回执不能复用于新尝试；旧 Session 与另一回执混用时，在 Verify/Cleanup 的 API 变更前
  拒绝。创建、验证和清理都检查回执绑定的 Namespace UID；清理仍使用 Job UID/RV 的删除前置条件。
- `RecoverCleanup` 不依赖原内存 Session，也不恢复或继续滚动升级。已确认 UID 的对象按 UID 清理；仅有
  创建意图时，必须重新查到带本次随机标记和精确源 owner 的 Job，持久化恢复出的 UID 后再删除。当前
  NotFound 不足以排除延迟 CREATE，因此返回明确未解决错误、保留回执，后续可对同一回执再次恢复。
  测试覆盖第一次恢复时缺失、原对象稍后出现、第二次恢复完成清理，未用新尝试掩盖旧创建的不确定性。
- 一个目标被替换/无法清理时，继续处理其他独立自有目标并汇总错误。Job DELETE 成功仍须等待依赖 Pod
  消失，之后才持久化已清理状态。回执失败/关闭不算成功；没有 UID 且 admission 又改掉所有权证据的
  不确定对象仍拒绝猜测删除。空 Journal 仍允许用于非持久化组件测试，未来部署 CLI 必须强制配置回执。

本轮验证与回执（仓库外既定审计目录）：

- 初次恢复测试被测试临时目录权限不满足 0700 拦住；修正 fixture 权限，未放宽产品目录要求。
- `security-image-prepull-recovery-binding-before.log` 为混用另一轮回执的 RED（原会话 18373 exit 1）：
  原先先清理对象再因目标不属于回执报错。现在先检查 attempt/目标/源/已知 UID 绑定，不发生任一轮的删除。
- `security-image-prepull-recovery-verified.log` 为软链接加载被错误接受的 RED；修复后加入锁软链接、
  硬链接、FIFO、目录越界、大小/格式/权限、状态回退和 UID 变更拒绝覆盖。
- `security-image-prepull-recovery-namespace-before.log` 为 Verify 未检查 Namespace incarnation 的 RED
  （原会话 26091 exit 1）；现在与创建/清理一样使用回执绑定的命名空间身份检查。
- 最终 `go test -race ./hack/production/internal/imageprepull -count=3 -cover` exit 0，5.580s，语句
  覆盖率 88.5%，日志 `security-image-prepull-recovery-final-verified.log`，原会话 7102 已终态。
  覆盖磁盘中的精确目标意图先于 API CREATE、UID 回执写入失败后重新打开恢复、进程内 Session 丢失、
  取消后的独立补偿、已确认 UID 在 admission 改 owner 后仍被清理、替换对象保留及其他目标继续清理。
- `go vet ./hack/production/internal/imageprepull`、`GOARCH=arm64 go build ./hack/production/internal/imageprepull`、
  gofmt/diff 检查通过；arm64 是交叉编译，不是 arm64 运行测试，也没有生成仓库内独立二进制。
  九文件源码清单 `security-image-prepull-recovery-code.sha256` 已逐项核对。

没有运行真实集群变更、push、镜像 CI 或新的产品提交。上述测试使用 fake Kubernetes API 与真实本地文件
操作，不是实际进程 SIGKILL/主机断电/真实 admission 的端到端证据。下一步继续补有界 API transport、
OCI 多架构发布证据、CLI/runner 中回执强制使用及准备状态即时复核、探针完整窗口，随后执行规定的完整
提交前后门禁和真实升级验收。不得将清理回执当作升级成功凭证，或复用旧 711 项回执覆盖本轮源码。

### 有界 API、OCI 平台映射和两个 CLI 入口（2026-09-09，未提交/未部署）

继续前已核对上一轮九文件校验和全部匹配且无旧测试进程。本轮新增有界客户端、发布索引验证器及
`hack/production/cmd/image-prepull`，目前组件十三文件、命令两文件，均未提交；没有修改 go.mod/go.sum、
Dockerfile、镜像 CI 或既有升级脚本。命令尚未打包进产品镜像。

`NewBoundedClient` 要求经过证书校验的 HTTPS，拒绝 URL 凭据/query/fragment、跳转和跳过 TLS 校验；
不接受无法保证遵循取消语义的外部认证插件、自定义 transport/dial/proxy 回调。支持原生证书、token/
token-file 和 in-cluster 配置。HTTP 尝试的超时上限 30s，完整响应体上限可设 1..32 MiB，在 Kubernetes
解码之前读取并核对上限，不能用合法 JSON 前缀掩盖超大尾部数据。关闭压缩请求，收到非预期编码则拒绝。
这里限制的是每次 HTTP 尝试和有限响应体，**不是整个 SDK 多次重试/退避的总时间**；后者仍须由操作/阶段
context 限定，执行器已有阶段期限。也不将此客户端用于 watch/log/exec 流，不宣称任意插件/文件系统调用
能被它强制中断。

`ApprovedRuntimeDigests` 校验原始索引字节的 SHA-256 必须等于批准的固定 image digest，再与独立审核的
CI amd64/arm64 子 manifest digest 逐项比较，要求两平台唯一且完整。允许明确关联到批准平台的 BuildKit
unknown/unknown attestation descriptor，但它们永不进入可运行 digest 清单。拒绝重复/大小写别名字段、
截断/尾部 JSON、过深嵌套、嵌套 index、额外架构、未知 CPU/OS 要求及混淆描述符。该组件实现本项目双平台
发布策略，不是通用 OCI resolver；依据 [OCI index](https://raw.githubusercontent.com/opencontainers/image-spec/v1.1.1/image-index.md)
和 [descriptor](https://raw.githubusercontent.com/opencontainers/image-spec/v1.1.1/descriptor.md) 的索引引用、
原始字节 digest/size 语义。**不下载或验证子 manifest/layer、不验证签名/attestation，也不认证任意输入
JSON 声称的 CI 结论或源 revision**；调用方仍必须从已核对的发布证据提供固定 image 和平台 digest。

CLI 现有模式与边界：

- `--mode=verify-release --index-file=<absolute raw index path> --image=<approved index image>`，配合
  `--amd64-digest` / `--arm64-digest` 输出每个平台的 child+index runtime digest 列表，并明确标记仅为
  身份验证，不是 CI 授权或升级成功。输入保留原始字节，不能先经 jq 重新排版后沿用原 digest。
- `--mode=recover-cleanup` 强制显式绝对 kubeconfig 路径、context、0700 回执目录/名称、Namespace
  名称/UID 和源 StatefulSet 名称/UID。先比较回执 scope，再读取 0600 的有界普通 kubeconfig 文件，
  正常解析其相对证书路径，使用有界 HTTPS 客户端执行清理；不用默认 context，不修改本机 kubeconfig。
  Scope 不符、Namespace UID 变化、上下文缺失、权限/输入异常都不能输出清理成功。所有出口释放回执锁。
- 尚未暴露 prepare/rollout 模式，也没有持久化“准备成功”凭证或升级前跨进程验证接口。清理回执不能
  被当作重启后继续升级的授权。这些入口没有在真实集群执行过恢复删除。

验证证据（仓库外既定审计目录）：

- 首轮有界客户端测试 `security-image-prepull-client-first.log` exit 0；覆盖 TLS、有限响应精确边界、
  已知长度/分块超限、合法 JSON 后超大尾部、响应头/正文超时、重定向不转发认证信息和非预期编码。
- 最终 `go test -race ./hack/production/internal/imageprepull ./hack/production/cmd/image-prepull -count=3 -cover`
  exit 0，日志 `security-image-prepull-client-release-command-final.log`，原会话 6761 已终态。
  组件 6.908s / 88.5%，命令 1.290s / 82.8%。CLI 使用真实本地 HTTPS 测试服务验证显式 context、
  Namespace incarnation、回执 scope、输入文件/权限、锁释放和成功/失败输出；恢复测试使用空清理回执，
  并断言仅发 Namespace GET，不将其冒充真实多 Job 的 CLI 端到端删除证明。
- `go vet` 两包通过；`GOARCH=arm64 go build -o /dev/null ./hack/production/cmd/image-prepull` exit 0，
  原会话 35922 已终态。这是交叉编译，不是 arm64 执行测试，没有在仓库生成二进制。
- 对历史 `security-de8a9e1f-image-manifest-by-digest.json` 执行新 CLI 的 verify-release：原始文件的
  SHA-256 精确为 `0044f89deef94bc6cebae6e548f3715152a35a5b83e63ed895575eccd3dc28c6`，对应已记录的
  amd64 `a83111ee...` / arm64 `50f47075...` 平台子 digest。命令 exit 0，原会话 35892 已终态，输出
  `security-image-prepull-existing-release-cli.json`。该步骤只读取历史本地发布证据，未访问集群、未重新
  触发 CI、未把 de8 镜像当作含有本轮新代码的镜像，更未关闭此前真实冷拉取升级失败。
- gofmt/diff 检查通过，十五文件源码清单 `security-image-prepull-client-release-command-code.sha256`
  已逐项验证。当前无需继续等待的本轮测试/编译会话。

下一步仍是准备状态跨进程即时复核、CLI prepare/verify 与 runner/镜像打包接入、全窗口探针预算及真实
admission/升级验收。完成集成后须运行新包/命令测试和规定的完整提交前后门禁，再推送构建镜像；旧门禁
回执不覆盖这些未提交源码。当前目标保持未完成，没有集群变更。

### 跨进程准备验证与 prepare/verify 命令（2026-09-09，未提交/未部署）

继续前已核对上一轮十五文件校验和全部匹配、无旧测试进程。新增准备快照、只读规划及 CLI 生命周期
测试，本轮组件十六文件、命令三文件，共十九个未提交文件；既有 runner、Dockerfile 和 CI 尚未修改。

回执现在可带可选 `preparation` 字段，不能再将当前格式描述为只含 cleanup 信息；旧的仅清理回执仍可
读取，但不能用于 Verify。该字段仍不是独立升级授权：

- 只保存固定目标镜像、源 spec/revision、RuntimeClass 和完整 Service 清单的摘要，以及节点身份/
  架构/runtime、原 Pod UID、重建 Job 的策略摘要、期限和经审核的 runtime digest。Job UID/attempt
  继续沿用同一持久化记录。不保存源 StatefulSet 原始配置、业务 env、证书/Secret 内容或拉取凭据。
- 准备阶段确认 holder 就绪后才持久化快照；落盘后再次做 live Verify，防止写盘耗时用尽活动窗口或
  期间已取消。Node runtime 版本现在必须非空且有界，不能用未知 runtime 身份制作准备快照。
- `RestoreVerified` 要求调用方重新提供批准的 image/platform digest，读取当前 Namespace、源对象、
  RuntimeClass、完整硬调度池和 Service 清单，比对摘要并重建 Job 策略；然后重新验证原 Job/Pod UID、
  Running/Ready、init 成功、imageID、源稳定 revision 和剩余持有期限。只发 GET/LIST，不能补建 Job、
  重跑准备或凭旧“ready”字段跳过即时检查。后续业务镜像修改仍须由 runner 做 UID/RV/spec 围栏。
- RuntimeClass 的无关标签/RV、Service 列表顺序和无关 metadata 不影响摘要；替换 UID、相关策略/selector
  变化、节点池变化及重建策略不一致都拒绝。进入清理时先持久化使准备快照失效，部分清理失败也不能把
  原尝试重新当作准备成功。该限制同时适用于重新打开回执和仍保留原 Session 的同一进程。

新增只读 `Plan` 从显式回执 scope 读取源对象和完整节点/Service 清单，生成有界随机 Job 名称并预先验证
每个请求；实际 CREATE 前执行器仍重新检查。CLI 现有四个模式，原 verify-release/recover-cleanup 保留：

- `--mode=prepare` 强制 `--confirm-create-isolated-jobs`、完整显式 namespace/source UID、kubeconfig/
  context、私有回执路径及经重新核对的原始 OCI index/两个平台 digest。只创建隔离 holder，不修改业务
  StatefulSet/Pod。成功持久化并再次验证后输出 `PREPULL_READY`，留下 holder 供升级流程使用；若输出
  成功标记失败，则在回执关闭前用独立上下文补偿清理。
- `--mode=verify` 重新打开相同回执并重新核对发布索引，再执行 RestoreVerified；只有即时检查通过才
  输出 `PREPULL_VERIFIED`。无确认创建参数也不会创建/替换对象。仅清理回执、失效快照、替换 Pod 或
  任何相关证据漂移均失败且不输出成功标记。
- 参数默认 planning/preparation 总预算 10m、单次 fresh verification 总预算 30s、holder 3600s、
  最小剩余持有时间 30m、TTL 300s；值都有边界验证。**这些默认值不代替 runner 的完整时长核算**：
  接入时需覆盖 probe 启动/完成、rollout/rollback、查询和清理预算。prepare/verify 并不自动触发升级，
  runner 还必须在所有出口调用恢复清理，不能假定 SIGKILL、进程崩溃或文件关闭异常能执行 Go defer。

测试及源码证据（仓库外既定审计目录）：

- `security-image-prepull-preparation-verified.log` 首批跨进程测试通过，涵盖源 spec/revision、runtime、
  Service selector、节点集合/身份、Job/Pod UID、过期窗口、目标镜像/approval 和策略摘要变化的拒绝，
  并检查全部 Kubernetes action 都为 GET/LIST。检查持久化内容不含业务配置/凭据名称等测试私有数据。
- `security-image-prepull-preparation-invalidation-before.log` 记录一个实际 RED（原会话 8042 exit 1）：
  清理使快照失效后，DELETE 不可用、holder 仍健康，原内存 Session 的 Verify 仍会成功。已增加活跃
  准备快照检查，禁止同进程绕过该失效状态；不是只修正文件重开路径。
- `TestCommandPrepareVerifyAndCleanupLifecycle` 使用本机真实 HTTPS 连接和 Kubernetes JSON/protobuf
  序列化，跨独立命令调用走通 prepare → verify → 拒绝被替换 Pod → recover-cleanup → 拒绝再验证。
  模拟服务端验证 DELETE 的 UID/RV/Foreground，断言全程只允许一次 Job CREATE、一次 Job DELETE，
  不得对业务资源写入；另验证缺少确认时零变更、输出错误时补偿清理。该服务模拟 admission、Job 运行
  和 GC，不是实际 apiserver/镜像拉取/真实控制器的证据。
- 最终 `go test -race ./hack/production/internal/imageprepull ./hack/production/cmd/image-prepull -count=3 -cover`
  exit 0，日志 `security-image-prepull-preparation-command-final.log`，原会话 79413 已终态。
  组件 8.360s / 85.1%，CLI 17.263s / 84.6%；这些分别是各包自身覆盖率，不是全系统覆盖率或生产验收。
- 两包 `go vet` 通过；arm64 命令交叉编译到 `/dev/null` exit 0（原会话 19933 已终态），无仓库二进制
  残留。gofmt/diff 检查通过，十九文件清单 `security-image-prepull-preparation-command-code.sha256`
  已逐项核对。本轮测试/编译均已终态，无旧进程待续。

本轮未访问或修改真实测试集群、未 push、未提交产品、未触发镜像 CI。下一步是用受控 scope 验证真实
admission/defaulting（不能将本地模拟结果视为已通过）、接入 runner 的准备/即时验证/EXIT 清理与完整
探针窗口，再完成镜像打包、规定的完整提交前后门禁、构建和真实升级验收。整体生产就绪目标仍未完成。

### 真实服务器端 dry-run 与 PriorityClass 修复（2026-09-09，未提交/未部署）

本轮先核对十九文件版本校验和，再增加 `check-admission` 模式。它使用完整只读 Plan，对每个节点依次
提交 Job 和 Pod 的 `CreateOptions{DryRun:["All"], FieldValidation:"Strict"}`，比较服务端返回的策略，
最后重查节点池。只检查 Job template 不足以覆盖 Pod admission，因此两类对象分别检查。该模式仅留下
本机私有目录内的空清理回执/锁文件，不绑定真实 CREATE attempt、不写准备成功快照、不执行容器。

使用显式既定 kubeconfig/context、Namespace UID `6c57c242-912b-41bb-9020-f4fdb3225ef3`、StatefulSet UID
`2650ad15-1d37-41c4-836c-d40dd4502720`，client Service `kubebrain-client`，对历史已核对的 de8 镜像索引
执行服务器端检查。操作只含 GET/LIST 和 DryRunAll CREATE；没有持久 Job/Pod 创建、镜像拉取、业务
镜像修改、worker/PD/TiKV 重启或存储类变更。

第一次真实检查 `security-image-prepull-admission-live-first.log` exit 1（原会话 92922），本机回执
`admission-check-01`。服务器拒绝 Pod：直接指定 `PreemptionPolicy=Never` 与默认 PriorityClass 计算出的
PreemptLowerPriority 不一致。源码生成器遗漏了源 Pod template 的 PriorityClassName；此前模拟测试没有
实现 Priority admission，因此未发现这个真实错误。依据
[Kubernetes v1.36.0 Priority admission](https://raw.githubusercontent.com/kubernetes/kubernetes/v1.36.0/plugin/pkg/admission/priority/admission.go)，
Pod 的 Priority/PreemptionPolicy 必须与具名类计算结果一致，不能仅写 Never 就改变类的策略。

只读检查确认源已经配置 `kubebrain-dbaas-critical`，无需创建或修改任何集群级 PriorityClass：

- UID `2b5d406a-0ae0-4e38-bcfc-9f34e7c18037`，观察到 RV `12439368`；Value `1000000`，PreemptionPolicy
  `Never`，不是 global default。完整只读证据 `security-image-prepull-admission-priorityclass.json`。
- 现在要求源明确使用已有、身份有效、未删除、用户优先级范围内的非抢占 PriorityClass，生成 Job 保留
  类名和匹配的优先级值。没有具名非抢占类、缺少 UID/RV、预留系统优先级、源 Priority/PreemptionPolicy
  冲突时明确拒绝；不自动改成抢占策略、不选择其他高优先级类、不自动创建集群资源。
- Plan/节点池复核会读取该类；Prepare 深拷贝并拒绝混合类快照，准备凭证新增 PriorityClass 摘要，跨
  进程 Verify 重查 UID、数值、抢占策略等。仅描述/RV 等无关 metadata 改动不误判为调度策略漂移。
  旧草稿中没有该摘要的 preparation 快照不能被新代码拿来继续升级；仅清理回执仍可用于恢复清理。

修复后的第二次真实检查 `security-image-prepull-admission-live-second.log` exit 0（原会话 76959），
本机回执 `admission-check-02`，输出 `PREPULL_ADMISSION_CONFIRMED dryRun=All containersExecuted=false`。
当前完整硬调度池的 Job/Pod admission 都通过；不表示容器能启动、镜像能拉取或滚动升级可用。

检查前、第一次失败后和第二次通过后的 namespace Job/Pod 名称+UID 清单分别为
`security-image-prepull-admission-inventory-before.json`、`...-after-first.json`、`...-after-second.json`，
两次 cmp 都 exit 0，清单完全一致。`security-image-prepull-admission-source-after.json` 确认源仍为 generation
3/observed 3、3 Ready、current/update revision 同为 `kubebrain-696c87f8f9`，继续运行旧镜像
`ghcr.io/fivetime/kubebrain@sha256:a245c95fea36c387358d86e3808a9d29073a327028d5a4e3a80e4d272663e865`。

本地验证：

- `security-image-prepull-admission-contract.log` 通过，逐个断言 Job/Pod 请求都有 DryRunAll/Strict，
  注入 Job/Pod 策略改变和最终取消均拒绝，tracker 不保存对象，回执没有 targets/attempt/preparation。
- PriorityClass 回归覆盖完整身份、名称/数值/策略保留、输入不别名、缺失/删除/抢占/系统优先级/冲突
  拒绝，以及准备后 UID/value/preemption 改变时拒绝恢复验证、仅 metadata 更新允许。旧默认值测试
  中人为写入 Priority=0 已修正为该测试类实际值 1000000，未放宽策略比较。
- 最终两包 `go test -race ... -count=3 -cover` exit 0，日志
  `security-image-prepull-admission-priority-final.log`，原会话 72125 已终态；组件 8.720s / 84.8%，
  CLI 17.288s / 82.3%。`go vet`（原会话 1520）与 arm64 编译到 `/dev/null`（原会话 40296）均 exit 0，
  gofmt/diff 检查通过；二十二文件清单 `security-image-prepull-admission-priority-code.sha256` 已核对。

本轮确实访问了真实测试 API，但仅进行了上述非持久 dry-run/只读检查；没有 push、产品提交、镜像 CI
或部署。真实 admission 缺口已对当前配置获得证据，仍需把准备、即时验证、EXIT 清理和完整探针窗口接入
runner，打包新命令，运行规定的完整提交前后门禁，再进行真实预拉取/升级验收。整体目标仍未完成。

### 预拉取命令打包与滚动探针窗口（2026-09-09，工作树未提交）

本轮继续推进发布流程，未创建上游 PR。二十二个预拉取组件/CLI 文件仍逐项匹配
`security-image-prepull-admission-priority-code.sha256`；没有更改此前已验证的 admission/准备/清理实现。

新增打包与门禁配置：

- Dockerfile 编译并在最终数据面镜像复制 `/usr/local/bin/kubebrain-image-prepull`。Go 二进制清单从
  66 增至 67；新增 build 测试检查最终 stage 中的清单、重复目标和非 root 用户，不把中间 BR 镜像混入。
- self-hosted image workflow 在发布前执行预拉取两包 race 测试及打包契约测试。发布后的检查配置为逐个
  amd64/arm64 子镜像运行实际 helper 的 `verify-release`，使用无网络、只读 rootfs、drop ALL capabilities、
  no-new-privileges 和只读原始 OCI index 挂载；比较精确 index/双平台 runtime digest 结果，再允许 promote。
  index 是公开元数据，设为 0444 供镜像内 65532 用户读取；没有挂载 kubeconfig/令牌或执行 prepare。
  此处仅新增 CI 配置，尚未触发构建，不能称为新镜像已验证。

runner 新增访问 Kubernetes 前的窗口校验：

- 默认 `PROBE_ITERATIONS=6000`、`PROBE_INTERVAL=0.1`，成功计数循环至少持续 600 秒；完成等待默认
  `PROBE_COMPLETE_TIMEOUT=900s`。原有单操作 5 秒、direct stream 30 秒、lease 5 秒等指标未放宽。
- 非 observe 模式要求 iterations × interval 严格大于 Ready wrapper、启动屏障等待、mutation wrapper、
  rollout/观察 wrapper 中的较大者，再加 2 秒余量。默认 70+90+15+310+2=487 秒。不能遗漏外层 310 秒，
  也不能依赖慢 RPC/冷拉取延长探针寿命。HardFailover 额外计入两次 leader exec、一次 Pod GET、UID 删除
  及其 kill grace，默认合计 596 秒；本轮只做本地模拟测试，没有执行真实硬故障。
- 正整数/十进制参数先按 Go Duration 精确值域验证，求和逐项防溢出，覆盖判断用除法而非溢出的乘积。
  单项 duration 合法但合计超过 int64 纳秒时拒绝。仅 observe 模式无升级窗口要求；运行期原 UID/phase
  覆盖检查和失败即回滚逻辑保持，不把配置通过视为可用性通过。
- runner 模拟 fixture 的计数由 3 改为 6000，summary 断言同步，仍不运行真实 6000 次业务操作；原有
  回滚、UID/RV、fixture 清理和故障断言未删除。历史真实 launcher 的 3600 次配置将被新校验拒绝，需要
  在下一次发布评审中重新配置，不能绕过 source checksum 直接复用。

截至本段写入的验证证据：

- `security-image-prepull-packaging-build.log`：完整 build 包通过（0.392s，真实 BuildKit 测试按既有开关未启用）。
- `security-image-prepull-packaging-race.log`：两包和 build 三轮 race 通过；组件 8.769s / 84.8%，CLI
  17.259s / 82.3%，build 2.690s；覆盖率不是全系统验收。对应会话 57741 已 exit 0。
- 两包/build 的 vet 和 arm64 helper 编译到 `/dev/null` 通过（会话 45371 exit 0）；没有仓库二进制残留。
- actionlint v1.7.12 通过，日志 `security-image-prepull-packaging-actionlint.log`；与 CI 相同 digest 的
  ShellCheck v0.11.0 容器只读检查脚本通过，日志 `security-image-prepull-window-shellcheck.log`（25162 exit 0）。
- 窗口边界与合计溢出测试三轮通过（55875 exit 0，1.725s），日志
  `security-image-prepull-window-boundaries-final.log`；覆盖短窗口、精确边界、纳秒小数、各阶段预算、
  大乘积、最大合法 interval、hard-failover 附加成本与 observe 模式。
- inventory verifier 通过（47517 exit 0）：712 项，四片 `171/195/183/163`。这只是分片清单验证，
  不是四片测试运行通过。本轮未执行完整提交前/后门禁，也未做产品提交。
- 首轮 runner 开发测试 85235 exit 1（553.774s），日志 `security-image-prepull-window-runner.log`。
  该轮启动后仍修改了脚本/测试，产生旧测试边界与新预算不一致，并有 Bash 运行中读到改动后文件偏移的
  syntax error；不是冻结输入的有效验收，失败记录保留。随后固定七文件
  `security-image-prepull-packaging-window-code.sha256`，重新启动完整 102 个 `TestRolloutAvailabilityRunner*`
  回归组：会话 **31040**，日志 `security-image-prepull-window-runner-final.log`，当前待终态确认。
- 未改动的实际探针包已通过：会话 **7740 exit 0**，日志 `security-image-prepull-window-probe.log`，
  163.012s。runner 31040 仍在运行；后续必须复核该原句柄/日志和冻结校验和，不因观察超时重启测试，
  也不把待运行结果写为通过。

本轮没有访问真实集群、push、CI 发布、镜像切换、PD/TiKV/worker 重启或存储变更。仍待把 prepare 放到
probe/fixture 生命周期之前，把 fresh verify 放到业务 patch 前，并在成功、失败和回滚 EXIT 中恢复清理。
接入 verify 后必须继续增加相应窗口预算；holder 的整个准备后运行/回滚/清理剩余寿命还需独立计入，当前
487/596 秒仅是现有探针覆盖窗口，不是 holder 生存期证明。之后仍须完整提交前后门禁、新 CI 镜像和真实
预拉取/升级验收；整体生产就绪目标仍未完成。

### 预拉取接入 runner 与完整提交前门禁（2026-09-09 10:48 UTC，未提交/未发布）

上一轮会话 31040 已确认 exit 0：冻结输入上的完整 102 个 `TestRolloutAvailabilityRunner*` 通过，
556.635s，日志 `security-image-prepull-window-runner-final.log`。该结果证明上一轮窗口/打包版本，不能
替代本轮新增 runner 接入的验证。此前 7740 的 probe 包结果仍为通过（163.012s）。

本轮新增 `hack/production/rollout-image-prepull.sh`，由 runner source，最终镜像的既有 `*.sh` COPY
包含它；新增 `rollout_image_prepull_test.go`。生产接入点为：

1. 候选发布在 Kubernetes 调用前检查显式单一 0600 kubeconfig/context、namespace UID、私有 0700
   目录、已构建绝对 helper 路径和双平台/index 证据。offline verify-release 使用有界输出/进程，
   `TARGET_RUNTIME_DIGESTS` 必须与已核对双平台和 index 的集合完全一致；不把任意 CI JSON 当作授权。
2. 在任何 fixture/probe 创建前生成独占私有子目录并打印回执位置，再调用 prepare。Go helper 接收
   绑定的 Namespace/StatefulSet UID、原始 index/独立平台摘要和保守 lifetime；未完成准备则不启动
   业务探针、不修改 StatefulSet。目录/receipt 不重用、不在退出时删除，供审计和死亡后恢复清理。
3. 探针启动屏障后、业务 patch 前调用 fresh verify，再 GET StatefulSet 确认初始 UID、完整 spec、
   revision、Ready 数仍一致，允许只改变 status 的 RV 并将新 RV 用于条件 JSON Patch。新增候选窗口
   为 539 秒（restart 487、hard-failover 596 不变）；仍保留运行中的 UID/phase 覆盖检查和原 SLO。
4. 成功时先清理 holder 再宣告 gate 成功；失败时先在拥有观察器的父进程终止并回收它，再隔离运行
   原回滚/fixture 清理，最后独立 recover-cleanup。清理不确定返回非零、打印 CRITICAL，保留回执。
   SIGKILL/断电不承诺自动补偿；需从打印的私有子目录、receipt-name=attempt 和原作用域显式恢复清理。

预算按当前 runner 有界调用/循环保守计入 probe 完成与三轮 fixture、rollout/rollback、UID 删除循环
及单次调用超期、剩余 API 调用和 replica inventory。默认 minimum remaining 为 **10958 秒**，总 holder
lifetime 为 **11618 秒**；超出 24 小时的配置在乘加前拒绝。该计算假设本地主机调度能进展，不是停机/
断电等任意 wall-clock 延迟的保证；真实 fresh verification 和在线探针仍必需。

本轮保留的失败与修正证据：

- 接入首轮小组会话 51584 exit 1，仅 offline helper 模拟调用错误地写入 Kubernetes 日志，导致“预检
  无 API 调用”断言失败；日志 `security-image-prepull-runner-integration-first.log`。已将模拟 release
  日志单列，保留 no-Kubernetes 断言，而非删除断言。
- 第一个 lifecycle 小组 98980 exit 0（58.281s）；随后代码审查发现旧 cleanup 被隔离到子 shell 后，
  无法回收父进程的 rollout observer。四个已启动但未完成的 718 项 pre shards 因该已确认设计缺口
  主动终止：22726/85073/94501/3846 均 exit 143。终止前解析并核对了这些本地测试的进程树/进程组；
  终止后已核对原进程、后代与运行中 runner 均不存在。没有操作真实集群进程。这批日志
  `security-image-prepull-runner-integrated-pre-shard-{0,1,2,3}.log` 保留，不作通过或正式提交门禁证据。
- 新父进程回收测试先 RED（85192 exit 1，`security-image-prepull-runner-parent-reap-before.log`），
  明确输出 `OBSERVER_NOT_REAPED_IN_PARENT` / `OBSERVER_STILL_LIVE`；修复为父进程先 stop/wait 后，
  回收和 EXIT containment 测试连续十轮通过（64058 exit 0，0.204s，`...-parent-reap-after.log`）。
- 关闭 runner stdout 的回归先 RED（80623 exit 1，`security-image-prepull-runner-receipt-output-before.log`）：
  receipt echo 失败却仍调用 prepare。现在显式检查输出返回值，成功输出后才标记已开始准备；失败时
  不调用 prepare，也不把不存在的准备尝试误报为需要恢复清理。
- 修正后完整 `TestRolloutImagePrepull*` 小组通过（7561 exit 0，59.623s），日志
  `security-image-prepull-runner-lifecycle-final.log`。涵盖正确阶段顺序、准备/复核失败、假成功 marker、
  源 spec 漂移、status-only RV 刷新、清理失败后的回滚、作用域/摘要/预算缺失、有界挂起、输出失败、
  原错误码保留以及父进程回收。这里的生命周期是本地模拟 Kubernetes/helper，非实际创建 holder 证据。

其他验证及真实 API 范围：

- 用临时构建的真实 helper 和已核对的历史 de8 index 执行 runner `PREFLIGHT_ONLY=true`，会话 14066
  exit 0，日志 `security-image-prepull-runner-real-offline-preflight.log`。仅离线 release/作用域文件/
  timing 检查，没有 API 调用、没有准备回执或 holder 创建。
- 使用新预算在显式测试 scope 执行真实 `check-admission`，日志 `security-image-prepull-runner-admission.log`
  exit 0，`PREPULL_ADMISSION_CONFIRMED dryRun=All containersExecuted=false`。本机空回执
  `admission-runner-budget-01` 保留。所有 Job/Pod 名称+UID 投影的 before/after JSON cmp exit 0；无持久
  Kubernetes 对象新增、无容器运行。源仍为 UID `2650ad15-1d37-41c4-836c-d40dd4502720`，generation/observed
  3、3 Ready、revision `kubebrain-696c87f8f9`、原 a245c95f… 镜像，没有业务镜像切换。
- 临时 helper `/root/.local/state/kubebrain/tk-001-003/prepull-runner-check.axCcqedJ/kubebrain-image-prepull`
  已在确认该精确普通文件后删除，空临时编译目录也用 rmdir 移除；可从源码重新编译。未清理共享 Go 缓存，
  未删除私有证据、回执或任何业务数据。
- 组件/CLI/build 三轮 race 通过（49675 exit 0）：9.002s / 84.7%、17.348s / 82.3%、2.677s，日志
  `security-image-prepull-runner-integrated-go-race.log`；root production、两新包、build 的 vet 通过。
  二十二个 Go 文件仍匹配 `security-image-prepull-admission-priority-code.sha256`。
- ShellCheck 起初仅报告 source 文件跨文件变量的作用域/未使用提示；用显式参数断言和返回变量明确
  helper 接口，并只对交由 caller 消费的单个返回值标注 SC2034。最终相同 CI digest 的 v0.11.0 检查
  通过（`security-image-prepull-runner-shellcheck-v2.log`），Bash syntax/diff check 通过。

**当前正在运行、尚未计作通过的完整提交前门禁：**

- 精确 `hack/production/test-shard.sh --verify 4` 已通过（95875 exit 0），720 项，分片
  **174/197/184/165**，日志 `security-image-prepull-runner-integrated-v2-pre-inventory.log`。
- 四片已同时启动：shard 0 → **89292**，shard 1 → **75783**，shard 2 → **75047**，shard 3 → **63999**。
  各自运行精确 `hack/production/test-shard.sh N 4`，日志
  `security-image-prepull-runner-integrated-v2-pre-shard-N.log`。后续必须先续查这些原句柄和日志，不能把
  暂时无输出或观察超时当作终态，也不能因跨会话而重启重复任务。
- 冻结九个接入/打包文件的清单为 `security-image-prepull-runner-integrated-v2-code.sha256`；执行期间
  不修改产品源/测试/脚本，终态后逐项核对。组件二十二文件清单单独保留；文档变更另行记录。

当前仍在 dbaas，HEAD 5c05e037，未做本轮产品提交、push 或镜像 CI。只有全部新门禁终态通过且源摘要
不变后才能提交，提交后还必须再跑 verifier 和四片；然后才准备新 CI 镜像和真实预拉取/全窗口升级验收。
本轮没有 PD/TiKV/worker 重启，没有操作 rook-ceph-secondary，也没有把 dry-run 或模拟成功当作生产就绪。

### 接入版本完整提交前门禁终态（2026-09-09 11:01 UTC）

本轮从原句柄继续确认上一节的 v2 pre gates，没有重新启动重复测试：

- shard 0 / 174 项：89292 **exit 0**，Go 469.375s。
- shard 1 / 197 项：75783 **exit 0**，Go 532.791s。
- shard 2 / 184 项：75047 **exit 0**，Go 397.942s。
- shard 3 / 165 项：63999 **exit 0**，Go 761.466s。

720 项 verifier 及全部四片现在均为终态通过；九文件 v2 接入清单与二十二文件组件清单再次逐项通过
sha256sum 检查，diff check 通过。此前主动中止的 v1 四片仍为 exit 143，不参与本次通过判定。
远端只读复核确认 origin/dbaas 仍是 de8a9e1f2ce9f5c0a0de802a98d79af871a12e20；最新镜像 CI 仍为
34307884732 的历史成功结果，本轮尚未 push 或触发新构建。本节记录允许准备本地产品提交，不是发布、
真实预拉取或全窗口升级通过；产品提交后必须立即重新执行 verifier 和四个完整分片。

### 本地产品提交与提交后门禁（2026-09-09 11:05 UTC）

在上一节全部 pre gates 终态通过、31 个产品/测试/构建文件摘要一致后，已创建本地提交
**05032758792028c2b78d190fd8fb703eba787df1**：
`production: gate candidate rollouts on verified isolated image preparation`。
包含 33 个文件（含两份文档）；提交当时工作树干净，dbaas 比 origin/dbaas 超前 4 个提交。
没有 amend/force-push，没有创建上游 PR。root@kubebrain.cloud.local 是本地既有提交身份，不代表已满足
TiKV 上游的贡献者身份或 DCO 要求。

产品提交后已立即重新启动要求的完整门禁，源文件再次匹配 v2 九文件清单及二十二文件组件清单：

- `hack/production/test-shard.sh --verify 4`：89789 **exit 0**，720 项，174/197/184/165，日志
  `security-image-prepull-runner-integrated-v2-post-inventory.log`。
- 四个完整 post shards 同时运行：0 → **46664**；1 → **96254**；2 → **44209**；3 → **14551**。
  命令分别为精确的 `hack/production/test-shard.sh N 4`，日志
  `security-image-prepull-runner-integrated-v2-post-shard-N.log`。本节写入时尚未终态，不计为通过；下轮
  必须续查这些原会话及日志，不能仅凭没有输出重启，也不能复用 pre gate 结果冒充 post gate。
- 提交后组件/CLI/build race 小组已通过（81031 **exit 0**，3.811/6.505/1.618s），日志
  `security-image-prepull-runner-integrated-v2-post-go-race.log`。

已核对部分 production 测试会读取 `docs/production_readiness_cn.md`，因此不能泛称所有文档都不是测试
输入。该说明文档在 05032758 提交后的门禁期间保持固定；提交后只向本测试状态文档追加结果和句柄，
不修改产品文件、测试或被上述断言读取的 production_readiness 文档。

本轮只有本地 Git 提交与验证；远端仍为 de8a9e1f，未 push、未触发新镜像 CI、未创建实际 holder 或
切换业务镜像。待全部 post shards 终态通过、摘要仍一致后，才保存最终回执并推送 dbaas、跟踪新 CI，
随后重新核对测试集群健康/存储保留策略并开展真实预拉取和完整可用性升级验收。总体生产就绪目标保持未完成。

### 05032758 提交后完整门禁通过（2026-09-09 11:17 UTC）

本轮继续等待原 post 会话，全部终态通过，没有复用 pre 结果，也没有在等待时修改产品代码：

- shard 0 / 174 项：46664 **exit 0**，Go 470.570s。
- shard 1 / 197 项：96254 **exit 0**，Go 531.398s。
- shard 2 / 184 项：44209 **exit 0**，Go 401.189s。
- shard 3 / 165 项：14551 **exit 0**，Go 758.523s。

与 89789 的 post verifier、81031 的 post race 结果一起，05032758 的规定提交前/后门禁现已完整通过。
再次核对九文件 v2 清单与二十二文件组件清单全部匹配；与 HEAD 的非 docs 路径无差异、无未跟踪产品
文件，diff check 通过。私有机器可读回执为
`security-image-prepull-runner-integrated-v2-post-complete.json`；它只证明本地提交源验证，不是 CI/镜像/
实际准备或升级通过。

等待期间执行一次只读后台健康检查（68604 exit 0），日志
`security-image-prepull-post-wait-backend-health.log`：显式既定 kubeconfig/context/namespace，经已审核
PD diagnostic UID 的 wrapper 执行 GET 和 Pod 内只读 curl/df；3 PD、3 TiKV、连续三次 Region 样本无
异常，六个 PD/TiKV 数据 PVC/PV 绑定、CSI 身份不重复、Retain 策略、容量隔离和磁盘阈值检查通过。
未重启或修改任何组件/存储。此为当时状态证据，正式执行升级前须再检查，不能拿旧健康结果长期放行。

旧 `run-security-watch-client-upgrade.sh` 保持未修改；它绑定 de8a9e1f/旧门禁及 3600 次探针，不可绕过
其 checksum guard 复用。新的演练入口必须绑定新 CI 的精确源码、已核对 index/平台摘要、预拉取回执
与 6000 次探针，并预先构建恢复工具，不能在关键清理窗口隐式下载/编译。保存本节文档后即可推送 dbaas
并跟踪 self-hosted 镜像构建；在新 CI 和镜像核验通过前不执行真实候选升级。

### 推送与新镜像 CI（2026-09-09 11:19 UTC，尚未部署）

已将完整门禁记录保存为 docs-only 提交 **4f9a3eb19e2dc42278d37b6af938246d68e269f1**，与产品提交
05032758792028c2b78d190fd8fb703eba787df1 的非 docs 路径无差异。检查工作树干净后，以普通 fast-forward
push 将 origin/dbaas 从 de8a9e1f 推进到 4f9a3eb1（推送会话 12358 exit 0），无 force push。

push 已自动创建新的 self-hosted image workflow：

- run **34344914914**，source **4f9a3eb19e2dc42278d37b6af938246d68e269f1**。
- URL：<https://github.com/fivetime/kubebrain/actions/runs/34344914914>。
- 创建时间 `2026-09-09T11:19:12Z`，首次观察状态 queued，尚未获得成功或发布证明。
- 最新只读快照记录到 `security-prepull-candidate-ci-current.json`；后续应通过原 run ID 查询 authoritative
  status/jobs，不另触发重复 workflow，不把 queued/in_progress 当作镜像通过。

本节是本地未提交的进行中状态记录；CI 运行期间不再为状态更新 push，避免 cancel-in-progress 取消
本次正在验证的源码构建。旧升级 launcher 保持不动；待本次 CI 成功且镜像的精确 index/双平台/源码/
工具核验完成后，再建立新演练回执和执行入口。当前没有实际 holder、候选 Pod、业务镜像变更或后端重启。

### CI 排队时的 Runner 状态（2026-09-09，待确认自动上线策略）

对原 run 34344914914 的只读查询显示 job **102444122834** 仍 queued，runner_id=0、runner_name 为空、
labels=[self-hosted]，尚未分配执行器。仓库 `actions/runners` 列表当时 total_count=1，列出
**raas-1534 / id 38 / offline / busy=false**，标签为 raas-ubuntu-24.04、self-hosted、linux、x64。
这些状态不能被解释为构建失败或永久不可用，也不能假设 Runner 一定会自行上线。

已向用户询问该 Runner 是否自动上线、还是需要用户启动。未修改 runs-on、注册新 Runner、读取注册
token、重触发 workflow 或自行登录未授权的 Runner 主机；保留原 run，后续继续查询同一 job/run 和
Runner 的实际状态。本地产品及完整门禁均已完成当前发布步骤，但镜像构建、核验和真实升级尚未完成。

### 候选升级入口与预编译恢复工具（2026-09-09 11:40 UTC，未执行升级）

继续查询同一个 run **34344914914**，状态仍为 queued、源码仍为 4f9a3eb1；本次仓库 Runner 列表
已变为 **raas-1534-r2 / id 39 / offline / busy=false**。这是执行器记录变化，不是构建开始或成功的
证据；未自行注册/启动 Runner、重触发 CI 或推送状态文档，保留原任务等待执行。

在私有目录 `/root/.local/state/kubebrain/tk-001-003` 中完成以下发布准备，未改动产品代码：

- 新入口 `run-prepull-candidate-upgrade.sh` 默认 verify，只在显式 execute 且所有检查通过后运行真实
  rollout。绑定 4f9a3eb1 源码、05032758 的 720 项完整本地门禁、本次 CI ID、精确候选 index/双平台
  摘要、既定 namespace/source UID、当前服务镜像及 revision、rook-ceph 消费者存储与数据 PV Retain。
  固定 6000 次探针及现有 5s/30s SLO，不执行 hard failover，不修改旧演练入口。
- `prepull-candidate-tools/` 中预编译 image-prepull 与 uid-delete（构建会话 70361 exit 0，Go 1.26.8，
  `-trimpath`）。它们是下一次授权测试使用的恢复工具，不是镜像发布证明，也不放入 Git。
- 新 UID 删除 wrapper 强制指定测试 kubeconfig，避免原 Go CLI 在未传路径时优先使用环境中的
  in-cluster identity；拒绝调用方覆盖 kubeconfig。runner 继续传入明确 context 和 UID/RV 条件。
  工具摘要清单 `security-prepull-candidate-tools.sha256` 同时绑定两个二进制、UID wrapper、既有 PD
  只读 wrapper 和后端健康脚本；构建信息保存在 `security-prepull-candidate-tools-buildinfo.log`。
- 两个 CLI race 测试会话 **34437 exit 0**，uid-delete 1.906s、image-prepull 6.506s；日志为
  `security-prepull-candidate-tools-race.log`。产品源码与本次 CI 源码的非 docs 差异为空，原九文件及
  二十二文件门禁摘要再次匹配。这些小组检查不替代前述完整门禁。
- 新入口/UID wrapper 的 Bash 语法检查及固定摘要 ShellCheck v0.11.0 均通过；ShellCheck 容器只读
  挂载这两个脚本、禁网，不挂载 kubeconfig。日志 `security-prepull-candidate-entry-shellcheck.log`。
  wrapper 覆盖 kubeconfig 的负向检查返回 2，未调用删除操作。
- 缺少实际候选镜像回执的 verify 检查返回预期 **exit 1**，提示未访问集群；日志
  `security-prepull-candidate-entry-missing-image.log`。没有伪造镜像成功回执，也未绕过入口检查。
  入口摘要保存在 `security-prepull-candidate-entry.sha256`。

另行执行一次显式 kubeconfig/context 的只读 PVC 查询，确认 `pd-kb-pd-0..2` 与 `tikv-kb-tikv-0..2`
这六个入口使用的实际名称存在，全部 Bound，StorageClass 为 `nvme-rep3-rbd-pool`。这仅确认名称和
绑定状态，不替代入口执行时的新鲜 PV/CSI 身份、Retain、PD/TiKV/Region 健康检查。

本轮没有创建 holder/探针、切换业务镜像或重启后端。下一步仍需原 CI 实际成功，独立核对新发布镜像
后才生成 `security-prepull-candidate-image-verified.json`，再运行只读 verify 和真实准备/升级验收。
当前没有该镜像回执；发布准备完成不代表 DBaaS 的整体生产就绪目标完成。

### 当前验收总览校正与只读复查（2026-09-09，CI 仍排队）

`docs/dbaas_acceptance_status_cn.md` 原先只保存 A5788 的 kind 表格，并混有“KubeBrain 本体尚待部署”
的旧描述。本轮将 tk-001-003 当前服务版本、候选本地检查、CI 状态与开放验收单独置顶；旧 kind 表格、
单宿主/hostPath 限制和清理历史原样保留为明确的历史部分，未删除失败证据或宣称生产完成。

本轮显式 kubeconfig/context 的 GET 证据保存在私有目录：
`security-prepull-acceptance-serving-state.json`、`security-prepull-acceptance-pods.json`、
`security-prepull-acceptance-storageclass.json`、`security-prepull-acceptance-nodes.json`。
StatefulSet UID/generation/revision/旧服务镜像未变化；九个业务/后端 Pod 都 Ready，restart 0，
同组件三副本实际分布在 k8s3-worker1/2/3。三个 worker 均无 zone/region 标签，这不能证明物理故障域
独立。StorageClass 仍是消费者 rook-ceph driver/clusterID，默认 reclaimPolicy=Delete，不能将它误写为
Retain；只有经过检查的六个现存数据 PV 具有 Retain，未来新卷还需逐一验收。

原样执行 `validate-tikv-region-health.sh`，会话 **86330 exit 0**；日志
`security-prepull-acceptance-backend-health.log` 确认 3 PD、3 TiKV、连续三次 Region 检查无异常，
包含六个数据卷 CSI 身份、绑定、容量隔离、保留策略与磁盘阈值检查。该检查只有显式作用域 GET 和
既有 PD wrapper 的只读 curl/df，不执行后端重启或数据变更。日志 `max_disk_used_percent=90` 是阈值。

本地 etcd 仍为 `5cd9f4ee13801e18825d661e5005ae599460bc3a` 且工作树干净；KubeBrain go.mod 当前
etcd API/client/server 依赖均 v3.7.1。这只是固定对标版本的复核，不表示已查询上游最新版本。
最后查询原 run 34344914914 仍 queued，raas-1534-r2/id39 仍 offline；未重复触发 CI 或 push 文档。
本轮是当前证据与验收边界的更新，不是候选部署验收；所有产品文件和既有门禁输入代码保持不变。

### 发布执行器阻塞复核（2026-09-09 11:48 UTC）

连续多轮观察同一 CI 执行器不可用。本轮额外查询 workflow 和 job：image.yml 的 workflow 为 active，
run 34344914914 为 push 事件且源码仍为 4f9a3eb1；job 102444122834 仍 queued、steps 为空、
runner_id=0、runner_name 为空，尚未实际运行。仓库唯一 Runner 记录已从 id39/r2 变为
**raas-1534-r4 / id40 / offline / busy=false**，带有匹配的 self-hosted 标签。记录变化不证明上线，
也不足以判断执行器启动失败的根因；不把 job 的 started_at 字段误当作已执行步骤。

最新快照：`security-prepull-candidate-ci-current.json` 与
`security-prepull-candidate-runners-current.json`。新入口和预编译工具摘要再次匹配，产品文件无差异，
当前候选镜像核验回执仍不存在。本地准备、检查和当前环境证据已经就绪；当前发布/真实升级路径需要
可用执行器和成功发布的精确镜像，不能用旧候选、空回执、另一次重复 dispatch 或降低检查要求替代。

未获执行器运维授权，不注册/启动 Runner，不改 runs-on；等待用户恢复执行器或确认其恢复方式。
原 run 保留，不取消、不重触发；当前服务与 PD/TiKV 保持不变。恢复后先查询原 run 的实际状态，再
依据其真实结果继续镜像核验与部署，不假定 queued 已成功，也不因为观察间隔而重启构建。

### 用户恢复 Runner 后继续原 CI（2026-09-09 14:40 UTC 起）

用户确认 Runner 机器恢复后，只读查询证明原 run **34344914914**、原 job **102444122834** 已实际
进入 in_progress，startedAt=`2026-09-09T14:40:55Z`，源码仍为 4f9a3eb1。仓库 Runner 为
**raas-1534-r37 / id51 / online / busy=true**。Set up job、Check out source、Compute immutable image
metadata 已成功，当前 Set up Go for security checks 执行中；未取消或重触发原 workflow。

建立跟踪会话 **40222**，命令为 `gh run watch 34344914914 --repo fivetime/kubebrain --interval 30
--exit-status`，输出私有日志 `security-prepull-candidate-ci-resumed-watch.log`。会话运行中不表示最终
成功，后续须观察原句柄终态并读取实际各步骤结果。排队阻塞已经解除，镜像验收与真实升级仍未完成。

重新核对新入口、两个预编译工具、UID wrapper、PD wrapper、后端健康脚本及两组产品源码清单，全部
摘要匹配；与 CI 源码的非 docs 路径无差异。测试 kubeconfig 仍为 0600，私有证据所在磁盘可用约
273 GiB。构建期间继续保持产品源码与发布输入不变，状态文档暂不 push，以免 cancel-in-progress。

### 恢复后的 CI 失败与测试等待预算修复（2026-09-09 14:50 UTC 起）

原 watch 会话 **40222 exit 1**，CI 34344914914 终态 failure。失败步骤为
`Verify isolated image preparation contracts`；两个子测试分别在 admission_test.go:111 和
preparation_test.go:64 的成功 Prepare 断言处得到 context deadline exceeded。完整失败步骤日志保存在
`security-prepull-candidate-ci-failed.log`。Go 安全扫描、双平台 kubectl 构建/扫描、目标架构与 manifest
selection 检查已通过；镜像构建/发布/提升步骤尚未执行，不能为本次源码创建镜像成功回执。

两个失败点均复用 `newExecutorFixture` 的 PrepareTimeout=1s；成功路径包含真实 journal fsync，
该短预算把共享 CI 调度/IO 延迟误当作被测身份或策略错误。具体 Runner 延迟来自 CPU、磁盘还是其他
因素未单独测定，不将推测写成基础设施根因。新增受控延迟回归在成功 CREATE 返回前等待 1.1s，原
配置稳定复现同样错误（会话 **79328 exit 1**，Go 1.332s，日志
`security-prepull-ci-fixture-budget-before.log`）。

仅修改 `hack/production/internal/imageprepull/executor_test.go`：共用成功路径 Prepare/Cleanup
测试预算调整为 30s；生产 Executor、CLI/runner 的 600s/30s 参数及 5s/30s 业务 SLO 均不变。新增
回归还显式配置 50ms 准备期限并延迟返回 100ms，必须得到 DeadlineExceeded、无 Session，且按 UID
补偿删除已创建 Job/Pod；不是将 deadline 错误改为成功。原有短预算失败用例继续使用自己的显式时限。

修复后组件与 CLI `-race -count=10 -timeout=5m` 会话 **2513 exit 0**，分别 37.679s/55.084s；日志
`security-prepull-ci-fixture-budget-after.log`。`go test ./build -run '^TestImagePrepull' -count=1 -v`
通过，日志 `security-prepull-ci-fixture-build.log`。修复文件摘要为 `security-prepull-ci-fixture-code.sha256`。

开始完整提交前门禁后不再修改产品或测试：inventory **2029 exit 0**，仍为
720=`174/197/184/165`；四片原句柄 **66742/65121/21060/29055**，日志
`security-prepull-ci-fixture-pre-shard-{0,1,2,3}.log`，当前运行中。新回归位于组件包，不计入 root
production 分片数量。待全部终态通过再提交，并立即执行规定的提交后门禁；之后新 CI 和入口必须
绑定新源码/新 run。旧失败 CI 与旧摘要回执保留，不能修改成通过。本轮没有任何集群变更。

提交前分片进展及启动环境校正：shard 1 / 65121 **exit 0**（553.086s），shard 2 / 21060
**exit 0**（404.661s）。shard 0 / 66742 **exit 1**（476.223s），唯一失败为原有 JWT 权限负向
测试的 permissive_private_key：本轮为了私有日志设置的 `umask 077` 被测试进程继承，使
`os.WriteFile(..., 0640)` 创建成 0600，无法触发其预期权限拒绝。未把该失败归因于本次预拉取修复。

对同一未修改用例做启动环境对照：077 再现失败（20031 exit 1，0.410s），022 下 count=3
通过（exit 0，0.162s），日志 `security-prepull-ci-fixture-umask-{private,default}.log`。不修改
JWT 产品检查或放宽断言；保留原失败记录，以外层 077 创建私有日志、仅测试子进程恢复本机原始
022 的方式完整重跑 shard 0，原句柄 **18739**，日志
`security-prepull-ci-fixture-pre-shard-0-default-umask.log`。其余正在运行的原分片不取消、不重启；
提交后门禁也将明确区分日志权限与测试进程 umask。当前所有产品及测试仍保持冻结。

### CI 测试预算修复：提交前完整检查通过（2026-09-09 15:57 UTC 复核）

继续查询原句柄后全部终态通过：shard 0 的默认 umask 复跑 **18739 exit 0 / 461.265s**，
shard 1 **65121 exit 0 / 553.086s**，shard 2 **21060 exit 0 / 404.661s**，
shard 3 **29055 exit 0 / 783.810s**。结合 2029 inventory 与 2513 十轮 race，当前修复具备
完整提交前证据；组件/CLI/build 的 `go vet` 也 exit 0，日志 `security-prepull-ci-fixture-vet.log`。
修复文件摘要匹配，其他非 docs 文件与 4f9a3eb1 无差异，diff check 通过。

接下来提交本轮唯一测试代码改动及累计交接文档，并立即跑提交后 inventory/四片；完整通过后再
普通推送 dbaas 触发新 CI。原 CI 34344914914 已失败，不会重写其结果或为它生成镜像成功回执。

### b48bef18 已提交，提交后检查运行中

本地提交 **b48bef184958f3abb817b79e6a5dcde5d148e6d5** 已创建，包含一个测试文件及两份累计状态
文档。与 4f9a3eb1 排除 docs/测试文件后的 diff 为空，即没有生产执行逻辑变化。远端仍为 4f9a3eb1，
新提交尚未推送，不存在新 CI ID；必须等当前提交后门禁完整通过再推送。

立即启动提交后检查，所有 Go 测试子进程使用原始 umask 022，日志由外层 077 创建：

- inventory **14682 exit 0**，720=`174/197/184/165`，日志 `security-prepull-ci-fixture-post-inventory.log`。
- 组件/CLI race 与 build 检查 **17407 exit 0**，4.968/6.499/0.010s，日志
  `security-prepull-ci-fixture-post-race-build.log`。
- 四个完整分片仍运行，原句柄 **49222/83112/29189/97273**；日志
  `security-prepull-ci-fixture-post-shard-{0,1,2,3}.log`。必须查询原句柄终态，不因观察中断而重启。

二十二文件组件源码新摘要为 `security-prepull-ci-fixture-component-code.sha256`，保留旧版本摘要
不覆盖。恢复工具在干净的 b48bef18 源码下重新编译（**52891 exit 0**），保存到私有
`prepull-b48bef18-tools/`；两二进制的 buildinfo 均为该完整 SHA 且 vcs.modified=false。
新 UID wrapper 保留强制 kubeconfig 加载、禁止覆盖的作用域限制。工具及相关 wrapper/健康脚本
摘要为 `security-prepull-b48bef18-tools.sha256`，buildinfo 为
`security-prepull-b48bef18-tools-buildinfo.log`。旧工具未删除，新工具尚未用于任何集群操作。

旧 `run-prepull-candidate-upgrade.sh` 仍绑定 4f9a3eb1/失败 CI/旧组件摘要，当前必须拒绝运行，不能
绕过它执行。新提交后门禁和新 CI 都通过后，才为新源码/新 run/新工具/实际镜像建立独立升级入口。
整体目标保持 active，真实候选发布与部署验收仍未完成。

### b48bef18 提交后分片超时调查（2026-09-09 16:13 UTC）

提交后 shard 0 **49222 exit 0 / 471.716s**、shard 1 **83112 exit 0 / 547.074s**、
shard 3 **97273 exit 0 / 773.275s**。shard 2 **29189 exit 1 / 397.301s**：
`TestRolloutAvailabilityRunnerKeepsProbeActiveThroughRollout/completed_at_rollout_boundary`
触发原有测试辅助程序的 10s 整命令上限。输出已包含 probe 提前完成拒绝及候选回滚、部分 fixture
清理信息，但不能由这些部分输出证明全部清理完成。没有推送，也没有改动被测源码或提高超时。

该单用例十次复验仍与其余分片并行时，出现两次相同 10s 超时（**32499 exit 1 / 73.769s**，
`security-prepull-ci-fixture-post-boundary-repeat.log`）。不能将第一次失败直接视为已消失。
待其他所有分片结束后，以私有 BASH_ENV 时间戳跟踪同一个纯 fake-client 用例，十次全部通过
（**27762 exit 0 / 63.524s**，`security-prepull-ci-fixture-post-boundary-isolated-trace.log`）。
十份 `rollout-boundary-trace.<pid>.log` 显示整条预检查、预拉取模拟、拒绝、回滚及清理返回约
6.332–6.361s，最后为 runner 预期的 exit 1，Go 用例正常通过；没有观察到清理卡住。跟踪仅用于
本机模拟测试，不涉及真实凭据或集群。并行资源争用是可能解释，尚未证明具体 CPU/IO 根因。

保持同一 b48bef18 源码与 10s 断言，在没有其他分片并行的条件下完整复跑 shard 2；日志
`security-prepull-ci-fixture-post-shard-2-isolated.log`。原失败日志不覆盖，只有完整分片终态成功
才能作为提交后通过证据；不得以十次聚焦用例替代 184 项分片。该环境敏感的测试超时记录仍保留。

### b48bef18 提交后检查完成，允许推送（2026-09-09 16:22 UTC）

原独立重跑句柄 **73507 exit 0 / 394.668s**，完整 184 项 shard 2 通过，日志
`security-prepull-ci-fixture-post-shard-2-isolated.log`。与 shard 0/1/3 的原成功结果一起，四片
均已取得终态通过；原并行失败及两次聚焦失败保留，不改写为成功，也不宣称已证明争用的具体根因。

补充完整组件/CLI/build 三包 race **71865 exit 0**，4.949/6.521/1.577s，日志
`security-prepull-ci-fixture-post-all-race.log`。两组源码摘要重新匹配，非 docs 工作树无差异、无未
跟踪产品文件。机器可读本地回执 `security-prepull-ci-fixture-post-complete.json` 绑定 b48bef18、
720 项、各次原句柄/时间、新旧两组失败警示与固定源码摘要；不证明新 CI、镜像或真实升级通过。

接下来以 docs-only 提交保存本节及累计提交后证据，再普通 fast-forward 推送 dbaas。该文档提交
不改变被验证的非 docs 路径；新 CI 必须绑定推送后的精确源码，而生产/测试门禁仍绑定 b48bef18。
新 CI 创建后再记录 run ID；不重新执行已经失败的 34344914914，不使用其旧绑定部署入口。

### 新源码推送与新 CI（2026-09-09 16:24 UTC 起）

提交后完整记录已保存为 docs-only **998b977eb5c2435012ba2e8113bfd3a3a6a7bad5**，与已验证的
产品/测试提交 b48bef18 的非 docs 路径无差异。普通 fast-forward push 会话 **60715 exit 0**，
origin/dbaas 从 4f9a3eb1 前进到 998b977e，没有 force push，也没有重跑失败的旧 CI。

push 自动创建 **34376521384**，createdAt=`2026-09-09T16:24:26Z`，完整 headSha 为上述
998b977e，URL：<https://github.com/fivetime/kubebrain/actions/runs/34376521384>。首次 queued，
随后实际 in_progress，Runner **raas-1561 / id52 / online / busy=true**。正在跟踪原会话
**6591**，日志 `security-prepull-998b977e-ci-watch.log`，只读快照
`security-prepull-998b977e-ci-current.json`。此刻没有完成/镜像发布证明，不能生成成功回执。

新私有入口 `run-prepull-998b977e-upgrade.sh` 独立绑定新源码、新 run、b48bef18 的完整本地门禁
与预编译工具、新组件摘要、当前既定服务/存储身份和 6000 次探针。旧入口全部保留不改。它要求
实际生成的 `security-prepull-998b977e-image-verified.json`；当前缺失时 verify 返回预期 exit 1，
并明确“no cluster access performed”。Bash 语法与固定摘要 ShellCheck 均通过，日志
`security-prepull-998b977e-entry-shellcheck.log` / `security-prepull-998b977e-entry-missing-image.log`，
入口摘要 `security-prepull-998b977e-entry.sha256`。

本节为推送后进行中状态，暂不再次 push 以免 cancel-in-progress 取消新 CI。新 CI 成功后仍需核对
实际 index/双平台镜像、源码标签/运行版本和工具身份，再执行入口 verify 与真实准备/升级。本轮未
创建 holder/探针、未切换业务镜像、未重启后端；生产就绪目标仍未完成。

### 34376521384 失败：短期限回归对 CREATE 时点的错误假设（2026-09-09 16:32 UTC）

原 CI 与 watch **6591 exit 1**，run 终态 failure；安全扫描、kubectl 双架构构建扫描、架构和
manifest 检查通过，镜像构建/发布步骤 skipped。日志 `security-prepull-998b977e-ci-failed.log`：
唯一失败为新增 `explicit_short_deadline_remains_enforced`，executor_test.go:175 期望一个 DELETE，
实际为零。该用例约 0.08s 结束，短于 CREATE 回调中的 0.1s 延迟；准备阶段 50ms 期限可在日志/
计划检查期间、到达 CREATE 前耗尽。此时返回 DeadlineExceeded 且不删除任何对象是正确行为，
先前“必然已 CREATE”的断言是本次新增测试的错误，不是生产实现缺陷，也不归责于 Runner。

仅继续修改同一个 executor_test.go：记录是否到达会成功创建 fake Job 的回调；已到达时仍严格
要求一次补偿删除，未到达时严格要求零删除，两条路径都必须无 Session/无残留 Job/Pod。新增
明确已过期的父 context 用例，确定性覆盖 CREATE 前拒绝；原 50ms 期限及 100ms 延迟不变，
1.1s 慢成功场景也保留，不扩大生产预算或降低 SLO。聚焦 race 十次通过，**31867 exit 0 / 13.468s**，
日志 `security-prepull-fixture-create-boundary-focused.log`。

组件/CLI/build 十轮 race 加 vet 正在执行，原句柄 **7685**，日志
`security-prepull-fixture-create-boundary-all-race.log`。修复源码摘要为
`security-prepull-fixture-create-boundary-code.sha256`。开始新提交前门禁后源码再次冻结；先单独
执行完整 shard 2（保持原 10s runner 断言不变），随后运行其余三片，避免上一轮已观察到的分片
并行资源敏感性；不能把旧提交的通过记录套用到本轮工作树。当前尚未提交/推送新修正。

998b977e 的私有入口和失败 CI 继续保留且不得运行；新成功镜像回执仍不存在。本轮没有任何真实
集群操作、业务切换或存储变更，目标继续 active。

本轮组件/CLI/build 十轮 race 加 vet 已终态通过（**7685 exit 0**），Go 时间分别为
37.701/54.948/6.279s；inventory **70021 exit 0**，720=`174/197/184/165`。
完整 shard 2 原句柄 **87187** 仍运行，日志
`security-prepull-fixture-create-boundary-pre-shard-2.log`；其余 shard 0/1/3 尚未启动，须在它
终态后继续，不能把当前小组通过写成完整提交前门禁通过。修复文件摘要仍匹配，产品和测试冻结。

### 创建时点回归修正：提交前分片继续（2026-09-09）

继续等待原 shard 2 句柄，**87187 exit 0 / 387.732s**，184 项完整通过，未修改该分片中的
10s runner 断言或测试内容。其终态后才启动其余分片：shard 0 → **26493**，shard 1 →
**35637**，shard 3 → **9763**；各自日志为
`security-prepull-fixture-create-boundary-pre-shard-{0,1,3}.log`，当前运行中。所有日志由外层
077 创建，测试子进程沿用 022。源码摘要与 diff check 再次匹配，当前仍只有同一测试文件及
两份状态文档未提交；尚未创建新提交、新 CI 或镜像回执。后续继续原三个句柄，不重启已完成的
shard 2，也不以旧提交的门禁结果替代它们。

### 创建时点回归修正：提交前完整通过（2026-09-09 16:57 UTC）

四个原句柄全部终态通过：shard 0 **26493 exit 0 / 462.739s**，shard 1 **35637 exit 0 /
518.674s**，shard 2 **87187 exit 0 / 387.732s**，shard 3 **9763 exit 0 / 748.459s**。
本轮没有失败分片、没有改动测试断言后复用旧结果。结合 70021 inventory、7685 十轮三包 race/vet，
当前修正具备完整提交前证据。冻结文件摘要匹配，其他非 docs 路径与 HEAD 无差异，diff check 通过。

接下来提交同一测试文件及累计交接文档，并立即按相同调度顺序执行提交后 inventory、完整 shard 2、
其余三个分片和组件检查；提交后完整通过前不推送、不触发新的 CI。两次失败 CI 的日志继续保留。

### a98b6db1 已提交，提交后门禁运行中（2026-09-09 16:58 UTC 起）

已创建本地提交 **a98b6db1a5ff2d186bb8730d1b59f0558ec60d9d**，仅修改同一测试文件及两份
累计状态文档，尚未推送。与 b48bef18 排除 docs 和测试文件后的 diff 为空，生产执行逻辑未变化。
二十二文件新摘要为 `security-prepull-fixture-create-boundary-component-code.sha256`，旧摘要保留。

立即启动的提交后 inventory **63365 exit 0**，仍为720=`174/197/184/165`；三包 race
**69350 exit 0**，imageprepull 4.980s、CLI 6.492s、build 1.589s。日志分别为
`security-prepull-fixture-create-boundary-post-inventory.log` 和
`security-prepull-fixture-create-boundary-post-race.log`。

完整 shard 2 原句柄 **58950** 当前运行中，日志
`security-prepull-fixture-create-boundary-post-shard-2.log`。它终态后才启动 post shard 0/1/3；
后面三片尚未启动，不能把 inventory/组件小组通过当成完整门禁完成。测试子进程仍为 022，日志
由外层 077 创建，源代码保持冻结；进行中只更新本交接文档与验收状态，不改产品/测试输入。
当前仍没有新 CI、新镜像回执或任何真实部署操作，继续原句柄即可。

提交后完整 shard 2 已由原句柄 **58950 exit 0 / 392.047s** 通过；之后才启动剩余 post
shard 0 → **91732**、shard 1 → **6952**、shard 3 → **59666**，日志
`security-prepull-fixture-create-boundary-post-shard-{0,1,3}.log`。这三个原会话当前仍运行，
不能提前记作通过；其终态全部成功后再保存新回执、推送并跟踪新 CI。源码和测试未变化，
没有重复运行已完成的 shard 2，也没有更改任何生产 SLO 或真实集群状态。

### a98b6db1 完整提交后门禁通过（2026-09-09 17:19 UTC）

原剩余三个句柄均已终态通过：post shard 0 **91732 exit 0 / 468.364s**，shard 1 **6952
exit 0 / 527.826s**，shard 3 **59666 exit 0 / 758.091s**；加上先前独立通过的 shard 2
**58950 exit 0 / 392.047s**，本轮前后各 720 项完整检查全部通过，没有失败分片或失败后复跑。
63365 inventory 与 69350 三包 race 结果仍有效，源码/测试始终冻结。

机器回执为 `security-prepull-fixture-create-boundary-post-complete.json`，绑定 a98b6db1、两组
源码摘要、前后各四片的原句柄/时间及调度/umask 边界；保留对旧提交/旧 CI 失败历史的提示，不
将本轮本地通过解释为镜像发布或真实升级成功。摘要再核对通过，非 docs 工作树无差异，未跟踪
产品文件为空。

恢复工具 `prepull-b48bef18-tools/` 的摘要仍匹配。当前 a98b6db1 与该工具编译提交 b48bef18
排除 docs/测试文件后的差异为空，因此生产 Go 源码、依赖及脚本均相同；可以保留其真实 b48bef18
buildinfo 并复用已验证工具，不伪称它们由 a98b6db1 重编译。新入口须额外检查这项非测试源码
一致性，未来若工具或依赖发生变化则必须停止复用并重新构建。

接下来以 docs-only 提交保存本节，普通推送新源码触发新 CI；新 run ID 以实际返回为准。此前
34344914914、34376521384 均为失败历史，旧入口保持不可执行状态，不建立任何虚假镜像回执。

### a98b6db1 已推送，新 CI 接手（2026-09-09 17:20 UTC 起）

提交后记录保存为 docs-only **84d22dcc424245128f05802a424ee598e7a20364**，与 a98b6db1 的
非 docs 路径无差异。普通 fast-forward push **30339 exit 0**，origin/dbaas 从 998b977e
前进到 84d22dcc。自动创建新 run **34382354317**，createdAt=`2026-09-09T17:20:58Z`，
完整 headSha 为上述 84d22dcc；URL：<https://github.com/fivetime/kubebrain/actions/runs/34382354317>。
初始 queued，随后 in_progress，Runner **raas-1562 / id53 / online / busy=true**。

原跟踪句柄 **19841**，日志 `security-prepull-84d22dcc-ci-watch.log`，当前快照
`security-prepull-84d22dcc-ci-current.json`。尚无完成或发布证明，后续继续查询同一 run/句柄，
不为状态文档再 push 取消本次 CI，也不重触发旧失败 run。

新私有入口 `run-prepull-84d22dcc-upgrade.sh` 绑定 84d22dcc、新 CI、a98b6db1 的完整前后门禁
和新组件摘要，沿用精确当前集群/旧服务镜像/rook-ceph 消费者 PV 身份与 6000 次探针。保留
b48bef18 工具的真实编译身份；新增显式非 docs/测试源比较，只有依赖/生产源完全一致时才允许
复用。这项 Git 比较与工具摘要已单独复核通过，不修改旧入口或旧回执。

Bash 语法与固定摘要 ShellCheck 通过；缺少
`security-prepull-84d22dcc-image-verified.json` 的默认 verify 返回预期 exit 1，确认未访问集群。
日志 `security-prepull-84d22dcc-entry-shellcheck.log`、
`security-prepull-84d22dcc-entry-missing-image.log`，入口摘要
`security-prepull-84d22dcc-entry.sha256`。当前没有实际 holder/探针或候选部署，等待真实 CI 与
镜像核验后再运行新入口；整体生产就绪仍未完成。

### 34382354317 预拉取检查已通过，进入镜像构建

继续跟踪原 run/句柄，GitHub 实际步骤结果确认 `Verify isolated image preparation contracts`
为 success；之前的安全扫描、双平台 kubectl 构建扫描、目标架构与 manifest selection 也已通过。
当前 `Build and push TiKV test image` 为 in_progress，完整源码仍为 84d22dcc。该轮已在 CI
环境验证两次测试修正，但不能因此标记整个 CI 或镜像发布成功。

最新结构化证据保存在 `security-prepull-84d22dcc-ci-current.json`。原 watch **19841** 仍运行；
下一步等待同一 run 的镜像构建、实际发布镜像核验和 promotion 全部终态，再独立审查镜像字节/
平台摘要/源码身份，之后才能建立镜像回执并进行集群验收。未重复 dispatch、未 push 进行中
状态文档、未创建 holder 或切换业务镜像。

### 34382354317 发布成功，开始独立镜像复核

同一 CI 的 job 已于 2026-09-09 17:52:59Z 完成，整体 success；原 watch 19841 exit 0。
完整结果和日志保存在 `security-prepull-84d22dcc-ci-completed.{json,log}`。
本轮未出现旧 run 的 Buildx 删除超时；仍有 action 的 Node 20 迁移及 Node API 弃用警告，
不将其隐藏为无警告构建。安全扫描、双架构实际发布核验及 promotion 均通过。

独立从 registry 取得的候选索引为
`sha256:b3b5c25ac815f5b9388be6aa3f987a3dd6c602e3378f4fb142b6fa1f822ca725`，
amd64 为 `sha256:705c7500b44cbd656172f626e26b552c66aac6b963aa5012db7a4d2e372d60b1`，
arm64 为 `sha256:ab8455575aa40b55dd883af911271b45ba78f834f599262d3ed063cebfbad371`。
原始索引 `security-prepull-84d22dcc-index-observed.json` 已通过本地 verify-release，
仅证明索引/平台身份，不是部署成功。开始本地 amd64 拉取（句柄 37460，日志
`security-prepull-84d22dcc-amd64-pull.log`）以检查实际二进制；镜像成功回执仍未建立，
尚未创建 holder/探针或切换业务镜像。

独立复核后续：本地拉取 37460、实际 `--version`/配置检查 48717、抽取主二进制 buildinfo
68214 均 exit 0，临时检查容器已删除。版本/源码/构建时间、UID 65532:65532、fork 模块
`v2.0.8-0.20260909023231-832b70fd622f` 及 module sum 与预期一致；promoted tag 摘要一致。
据此建立私有 0600 `security-prepull-84d22dcc-image-verified.json`，不是部署回执。
精确入口摘要通过后，启动默认只读 verify，句柄 **97098**，日志
`security-prepull-84d22dcc-entry-verify.log`，证据目录
`prepull-84d22dcc-verify.BmZScfajzDiH`；继续等待同一进程完成，再做新候选 dry-run 准入。

只读 verify 97098 已 exit 0，输出 `CANDIDATE_UPGRADE_GUARDS_PASSED`、
`cluster_mutations=0`。精确资源/旧服务镜像/消费集群存储和后端健康检查通过；下一步为
新候选 Job/Pod 的 DryRunAll 准入核验，然后才是实际隔离准备与受控在线升级。

### 新候选准入通过，启动受控升级

新候选 DryRunAll 检查句柄 34519 exit 0，输出
`PREPULL_ADMISSION_CONFIRMED dryRun=All containersExecuted=false`；
`security-prepull-84d22dcc-admission-{before,after}.json` 的 Job/Pod 名称和 UID 比较 exit 0。
日志 `security-prepull-84d22dcc-admission.log`，私有 journal `admission-84d22dcc-01`。
使用 hold 11618s、minimum remaining 10958s，与正式入口预算一致；dry-run 不证明实际容器运行。

入口摘要再次通过后，启动 `run-prepull-84d22dcc-upgrade.sh execute`，活跃句柄 **93250**，
日志 `security-prepull-84d22dcc-upgrade.log`。入口会重新验证 CI/清单及精确集群身份/后端健康，
然后先做隔离镜像准备，再启动 6000 次探针并滚动业务镜像。此处仅记录已启动，不声明成功；
继续跟踪同一进程及失败时的回滚/UID 定界清理，不因观察超时重复启动。

实际准备已取得 `PREPULL_READY` 和升级前 `PREPULL_VERIFIED`。三个 holder 分别在三个
worker Running/Ready，CRI 实际 imageID 均为已审核的 amd64 子摘要。恢复 journal 为
`prepull.YczW7ofYlsNf/attempt`，执行证据目录 `prepull-84d22dcc-execute.Cj8orsCQAss0`。
探针 `prepull-upgrade-84d22dcc` 已 Running/Ready。最近读取 StatefulSet generation/observed
4/4、Ready 3、updated 2，目标 revision `kubebrain-59884d79b9`，目标镜像已改为本次索引。
升级正在执行，93250 仍活跃；上述是中间状态，不是全部副本完成或 6000 次探针通过。

后续只读观察：StatefulSet generation/observed 4/4、Ready/updated 3/3，current/update
revision 均为 `kubebrain-59884d79b9`；runner 输出 `ROLLOUT_PROBE_COVERAGE_CONFIRMED`，
探针 UID `14c0b2bd-b80b-4896-ac0c-29b194791f56` 仍 Running、restart 0。探针有连接切换
期间 Unavailable 重试警告；未取得最终 summary 前不能声明零失败或 SLO 通过。

已保存实际 holder 事件 `security-prepull-84d22dcc-holder-events.json`：三个节点的新索引
实际拉取耗时依次 39.065s、38.76s、34.873s，镜像大小 2049486220 bytes；不是仅把旧镜像
热缓存重跑视为预拉取修复。新业务 Pod 的精确 UID 清单和对应事件保存于
`security-prepull-84d22dcc-updated-pods-observed.json`、
`security-prepull-84d22dcc-updated-pod-events.json`；三个业务 Pod 均报告镜像已在节点本地。
这证明本轮拉取发生在隔离准备阶段，不证明所有镜像层此前均不存在，也不替代完整业务探针。

### 本次升级验收失败：探针完成超时，回滚与清理完成

原进程 **93250 exit 1**。第一条失败为 `availability probe did not complete within 900s`，
随后自动恢复原镜像；没有延长完成窗口或重复启动测试。最终日志同时包含
`FIXTURE_CLEANUP_OK status=absent ... keys=0 users=0 roles=0 leases=0` 和
`PREPULL_CLEANUP_CONFIRMED`。新鲜查询确认 generation/observed 5/5、Ready/updated 3/3，
current/update revision 均回到 `kubebrain-696c87f8f9`，镜像恢复原 a245c95f 摘要；
namespace 内名称含 prepull 的 Job/Pod 列表为空。journal 保留，不把清理成功视为升级成功。

清理前独立保留 `security-prepull-84d22dcc-probe-timeout.log`，最终错误为
`PROBE_FAIL iteration=4844: direct watch recovery exceeded 28.116701467s: context deadline exceeded`。
但不能倒置因果：`security-prepull-84d22dcc-rollback-events.json` 中候选 Pod kubebrain-2
于 18:21:46Z 开始被停止，kubebrain-1 于 18:22:42Z；该探针错误直到 18:23:43Z 才出现，
因此发生在回滚期间，而不是导致 runner 首次回滚的错误。没有完整 6000 次成功 summary。
下一步分析探针完成预算、实际逐次耗时及回滚期间 direct-watch 恢复问题；本次失败原样保留，
不得直接调大时限或把热缓存重跑当作冷拉取修复的全部证明。

后续源码检查确认每次迭代串行包含后端 TSO/Region/周期性 PD 检查、Put/public watch、
direct watch 验证，最后才 Sleep(interval)。6000×0.1s 只是间隔下界，不是总运行时长预测。
现有失败日志没有完成时限到达瞬间的迭代数和阶段累计耗时，无法据此认定某个具体后端变慢。

正在本地补充 `PROBE_PROGRESS` 诊断：最多 100 条中途记录加一条退出记录，含 UTC 时间、
已完成数、总耗时和已结束阶段的 backend/public/direct-wait/pacing 累计耗时；标记
`scope=diagnostic_only`，不替代成功 summary，不改变既有次数、间隔或 SLO 配置。
失败中的未完成阶段不会计入阶段累计，因此这些累计不必等于 elapsed。
`TestProbeProgressBoundedAndDiagnosticOnly` 定向测试 44703 exit 0；完整探针包 race
句柄 **47798**，日志 `security-probe-progress-package-race.log`，仍需核对终态。
当前修改未提交，完整提交前/后生产门禁尚未执行；不得用该工作树直接复用旧镜像发布入口。

诊断改动的提交前 inventory 88688 exit 0，仍为 720 项，四分片 174/197/184/165，
日志 `security-probe-progress-pre-inventory.log`；三个源码/测试文件摘要固定于
`security-probe-progress-code.sha256`。race 47798 已确认实际测试二进制仍在执行，
不是只有旧日志文件；待其终态再执行生产分片。冻结上述产品/测试文件，不能在门禁过程中改写。

完整探针包 race **47798 exit 0**，Go 用时 274.132s。源码摘要再次一致后，启动提交前
生产分片 2/4，句柄 **91163**，日志 `security-probe-progress-pre-shard-2.log`。
该时间敏感分片单独运行；其后再跑分片 0/1/3，全部终态成功之前不提交。

提交前分片 2 **91163 exit 0**，184 项、Go 用时 393.477s，源码摘要再次一致。
随后启动剩余分片 0 **1168**、1 **71019**、3 **24876**；对应日志
`security-probe-progress-pre-shard-{0,1,3}.log`。这些仍为运行句柄，不是成功证明，
待三个终态及摘要复核后才允许提交，随后立即执行完整提交后门禁。

提交前分片 0 **1168 exit 0**，174 项、Go 用时 467.099s；分片 1 **71019** 和
分片 3 **24876** 仍在原进程执行。尚不满足完整提交前门禁，不提交。

随后分片 1 **71019 exit 0**，197 项、Go 用时 523.054s。仅剩分片 3 **24876**
尚未结束；代码仍保持冻结。

提交前最后分片 3 **24876 exit 0**，165 项、Go 用时 755.754s。至此四分片全过，
耗时按 0/1/2/3 为 467.099/523.054/393.477/755.754s，inventory 720、174/197/184/165。
完整探针包 race 274.132s、go vet、git diff --check 及三个源文件摘要均通过。
该改动只增加诊断，不宣称已修复真实探针超时；提交后须立即执行完整四分片和 inventory，
全部成功前不推送触发 CI 或部署。

已提交诊断改动 **36545f8107ca9231db01df54d8f0b7f270679f2b**，未推送。
同一命令在 commit 后立即执行 inventory（720、174/197/184/165，成功后才继续）并启动
提交后分片 2，原句柄 **13758**，日志 `security-probe-progress-post-inventory.log`、
`security-probe-progress-post-shard-2.log`。后续依次完成剩余分片与探针包复核，当前没有
提交后完整成功回执，不能复用上一候选的发布/部署授权回执。

提交后分片 2 **13758 exit 0**，184 项、Go 用时 392.977s。源码摘要与非 docs 工作树
核对一致后启动分片 0 **73807**、1 **53421**、3 **79666**，日志
`security-probe-progress-post-shard-{0,1,3}.log`。三个均尚待终态；后续还须做提交后的
探针包复核并记录完整回执，不能提前推送或替换测试环境镜像。

提交后分片 0 **73807 exit 0**，174 项、Go 用时 475.650s。分片 1 **53421**、
分片 3 **79666** 仍在同一进程执行；发布条件尚未满足。

提交后分片 1 **53421 exit 0**，197 项、Go 用时 532.955s。仅剩分片 3 **79666**
仍在运行；其后执行探针包提交后 race 复核。

提交后最后分片 3 **79666 exit 0**，165 项、Go 用时 764.095s。四分片及 inventory
全部通过，按 0/1/2/3 的耗时为 475.650/532.955/392.977/764.095s。源码摘要一致后，
启动提交后完整探针包 race + go vet，句柄 **36555**，日志
`security-probe-progress-post-package-race.log`；当前仍待终态，尚未创建完整发布回执或推送。

提交后探针包 race + go vet **36555 exit 0**，Go race 用时 262.467s。
三个源码摘要、非 docs 工作树及差异检查再次通过，建立仓库外 0600 本地门禁回执
`security-probe-progress-post-complete.json`，绑定产品提交 36545f81、提交前后各 720 项
全分片、inventory 和探针包 race/vet。准备以 docs-only 提交保存这份终态记录后普通
fast-forward 推送 dbaas；新 CI 的完整 headSha 另行核对，不复用旧 84d22dcc 的镜像回执。

普通推送已成功：docs-only HEAD **34057ee8743c28753f9a078268a12783381dbe04**，
新 CI **34394834203**（https://github.com/fivetime/kubebrain/actions/runs/34394834203），
GitHub 返回精确相同 headSha，初始 queued。开始跟踪该 run，日志
`security-probe-progress-34057ee8-ci-watch.log`。当前没有新镜像回执，也未重新部署；
进行中的状态记录不再 push，避免 cancel-in-progress 取消本次 CI。

原 watch 句柄 **80863**；后续继续等待/查询同一 run，不因观察超时重复 dispatch。

CI 已 in_progress，当前步骤 `Set up Go for security checks`，源码仍精确为 34057ee8。
准备了私有 `run-prepull-34057ee8-upgrade.sh`，绑定新 run、36545f81 本地完整回执与新
镜像回执路径；保留旧 84d22dcc 入口和失败证据，不覆盖。原隔离组件/runner 摘要和本次
诊断三文件摘要共同检查。恢复工具保留 b48bef18 的真实编译身份：非 docs/test 源比较只豁免
本次另一个 package main 中的 probe main.go/progress.go，其余代码和依赖必须完全一致，
两个恢复命令不导入该 probe 命令。该比较已通过，没有虚构重新编译的工具身份。

新入口 Bash 语法及固定摘要 ShellCheck 检查通过；默认 verify 因缺少新镜像回执而预期
exit 1，并输出 `verified candidate image receipt is absent; no cluster access performed`。
日志 `security-probe-progress-34057ee8-entry-{shellcheck,missing-image}.log`，入口摘要
`security-probe-progress-34057ee8-entry.sha256`。所有次数、延迟门限、完成窗口保持原值，
当前尚未执行实际新候选准备或业务变更；完整 CI/新镜像核验后才继续诊断运行。

新 CI 已进入 `Verify published test image`，整体仍在运行。独立读取候选索引为
`sha256:f4fc874cf18633b2cc65afc52113d1c98f20bf63439929ea95238408cd94b637`，
amd64 `sha256:aec9ef743242966e3b6406cd7757ecb4900031e4ef573151da9430c9d9c12b60`，
arm64 `sha256:1cc4ad0aea4a87582c3871fa54daa1d6a8f4396737012d9a9ae234ebe82cc9c5`。
原始索引和本地 verify-release 结果保存于
`security-probe-progress-34057ee8-{index-observed,release-identity-observed}.json`。
开始本地 amd64 拉取，句柄 **43181**，日志 `security-probe-progress-34057ee8-amd64-pull.log`。
这只是独立清单身份核验，不是整体 CI 或部署成功；watch 80863 继续跟踪原 run。

CI **34394834203 全部步骤 success**，job completedAt 2026-09-09T20:08:16Z，原 watch
**80863 exit 0**。终态 JSON `security-probe-progress-34057ee8-ci-current.json` 与完整日志
`security-probe-progress-34057ee8-ci-completed.log` 保留 Node 迁移/API 弃用警告。
本地拉取 43181 exit 0，实际 --version/OCI 标签/UID 与预期一致，主二进制确认 fork 模块
版本及 module sum；promotion 标签独立读取也匹配新索引。检查容器已删除，二进制 buildinfo
和配置保留为 `security-probe-progress-34057ee8-image-*`。据此建立私有 0600
`security-prepull-34057ee8-image-verified.json`，不代表升级成功。

精确入口摘要通过后，只读 verify **96459 exit 0**，输出
`CANDIDATE_UPGRADE_GUARDS_PASSED ... cluster_mutations=0`。证据目录
`prepull-34057ee8-verify.JowrPs4iysVW`，日志 `security-probe-progress-34057ee8-entry-verify.log`。
下一步新候选 Job/Pod DryRunAll 准入，随后在原门限下取得真实阶段耗时；目前未创建新的
holder/探针或切换业务镜像。

### 34057ee8 原门限诊断升级已启动

新候选 DryRunAll 准入 exit 0，输出 `PREPULL_ADMISSION_CONFIRMED`、containersExecuted=false；
`security-probe-progress-34057ee8-admission-{before,after}.json` 的 Job/Pod 身份清单 cmp exit 0。
日志 `security-probe-progress-34057ee8-admission.log`，journal `admission-34057ee8-01`。
精确入口摘要通过后，启动 execute，原句柄 **12140**，日志
`security-probe-progress-34057ee8-upgrade.log`。保持 6000 次、0.1s 间隔、5s public/30s direct
门限和 900s 完成窗口，目的为取得阶段耗时，不预设本次能通过。继续跟踪同一进程，
先确认隔离准备，再观察周期性 PROBE_PROGRESS；失败时核对回滚与清理终态。

该进程已取得 `PREPULL_READY` 和 `PREPULL_VERIFIED`，三个 holder 分布于三个 worker，
探针 UID `0a4b86a5-c2be-415d-a4f0-4c8e870fc5e3` Running/Ready。恢复 journal
`prepull.xnVQZALLNELZ/attempt`，执行证据目录 `prepull-34057ee8-execute.i1WdMdTXoIDr`。
首条实际诊断（20:14:21Z）completed=60、elapsed=13668ms、backend=216ms、public=7076ms、
direct_wait=339ms、pacing=6035ms；已验证发布后的新探针实际输出诊断，非本地模拟。
短样本不能外推全程瓶颈或成功；日志 `security-probe-progress-34057ee8-probe-observed.log`，
原进程 12140 仍活跃，继续采集完整运行与回滚前时间线。

本轮 holder 事件已归档 `security-probe-progress-34057ee8-holder-events.json`，三个节点
实际拉取新索引耗时 41.279/43.081/40.276s。诊断到 300 次时 elapsed=118988ms，
backend=1151ms、public=33360ms、direct_wait=54291ms、pacing=30180ms，滚动窗口中
direct 等待累计出现跳升。随后 StatefulSet Ready/updated 3/3，current/update 均为
`kubebrain-df5f9d748`。这仍是中途数据，不是 6000 次验收通过；继续采集稳定阶段以区分
固定逐次开销和滚动恢复开销，不能用阶段累计直接替代每次操作 SLO。

稳定阶段短窗口 720→960：elapsed 差 49648ms / 240 次，约 206.9ms/次；public 差
23985ms（约 99.9ms/次）、pacing 差 24142ms（约 100.6ms/次）、backend 差 903ms、
direct_wait 差 616ms。runner 已输出 `ROLLOUT_PROBE_COVERAGE_CONFIRMED`。
该窗口支持“间隔之外仍有显著逐轮 public 路径开销”，但 public 合并了 Put 和 public watch，
不能直接归因为磁盘、TiKV、commit-wait 或 watch 轮询；也不能将短窗口外推为最终结果。
继续跟踪原 12140，未修改完成窗口或负载参数。

增加只读服务端指标采样：先校验探针 UID，再从该 Pod 用固定 info CA、TLS serverName 和
connect-to 指定三个业务 Pod，GET /metrics；不输出证书/私钥，不修改参数。
文件 `security-probe-progress-34057ee8-server{0,1,2}-metrics.txt`（最多 1MiB+1 捕获）。
server2 样本成功 Put sum=147.313096653s/count=1764，约 83.5ms/次；
write_commit_wait sum=33/count=1764，源码确认后者以整数毫秒直接观察（不是秒），
且 failure 两种原因均为 0。这提示继续细分 Put 路径，不能仅凭 public 约 100ms 就认定
commit-wait/watch 轮询主导；指标是副本生命周期累计，不是相同窗口的逐请求追踪，不能
归因为某个 TiKV/磁盘阶段。先前样本 1680 次 elapsed=431643ms，原测试仍运行。

server2 第二份指标保存为 `security-probe-progress-34057ee8-server2-metrics-later.txt`，
两份 process_start_time_seconds 相同，计数单调。成功 Put 增量 900 次、81.108457672s
（约 90.1ms/次），对应 apply 增量约 82.6ms/次；存储 commit 增量 1452 次、
72.459907482s（约 49.9ms/次），commit-wait 增量仅 10ms/900 次。
源码 `pkg/storage/metrics/store.go` 确认 etcd_disk_backend_commit_duration_seconds
测量的是 BatchWrite.Commit 调用，不是独立物理 fsync，且包含非 Put 提交；这些阶段嵌套，
不能直接相加或推断磁盘故障。继续从写入执行路径分析，而不是放宽完成时限。
探针中途到 2700 次时 elapsed=653597ms，原 12140 仍在执行，尚无完整成功 summary。

### 34057ee8 再次达到完成时限，正在回滚

runner 首次失败仍是 `availability probe did not complete within 900s`，随后自动恢复原镜像。
事件保存于 `security-probe-progress-34057ee8-rollback-events.json`：首个候选 Pod
kubebrain-2 于 **20:31:57Z** 开始停止。最后明确早于该事件的周期样本为 **4560 次**，
20:31:45Z、elapsed=1057451ms，backend=16655ms、public=496336ms、
direct_wait=85627ms、pacing=458786ms。4620 次与回滚处于同秒，4680 次及之后已经
在回滚期间，不能归入纯候选稳定阶段。900s 是 rollout 完成后窗口，不是整个探针启动时长。

独立探针日志保存在 `security-probe-progress-34057ee8-probe-timeout.log`；目前仍持续到
5040 次，没有最终 success summary，不能因后续操作成功覆盖首次超时失败。原进程
**12140** 仍活跃，generation 已变 7、目标回到旧 revision，继续确认全部恢复和隔离资源清理。

### 34057ee8 最终恢复与清理确认（2026-09-09）

后续复核时原进程句柄 12140 已不可查询，不补造其退出码。执行日志保留首次
`availability probe did not complete within 900s` 失败，末尾已输出
`FIXTURE_CLEANUP_OK status=absent owner_uid= keys=0 users=0 roles=0 leases=0`
及 `PREPULL_CLEANUP_CONFIRMED`。
使用固定 kubeconfig/context 的新鲜只读查询确认 StatefulSet generation/observed 为 7/7，
Ready/updated 为 3/3，current/update revision 均为 `kubebrain-696c87f8f9`，
镜像回到原始 `sha256:a245c95fea36c387358d86e3808a9d29073a327028d5a4e3a80e4d272663e865`。
同命名空间 Job/Pod 中名称含 prepull 的资源集合为空；没有重复启动升级。

结论：CI 发布成功、隔离预拉取及实际滚动覆盖已验证，但本轮 6000 次在线升级验收失败，
不能标记兼容性或生产就绪完成。诊断证明 100ms 间隔之外还有显著串行处理耗时，
不能将 6000×100ms 当作完整运行预算；Put/存储提交耗时的进一步因果定位仍待完成，
不以直接增加超时或降低负载覆盖失败。私有原始日志继续保留供复核。

### 客户端指标缺失定位与待提交修复

复查上述 server2 两份指标文件分别为 551788/552218 字节，均低于捕获上限，
末尾为完整 write_responsesize 样本；两份均未出现 tikv_client_go 请求/事务/退避指标。
当前 fork 的 metrics 包 init 只初始化 collector，RegisterMetrics 由嵌入应用负责调用；
KubeBrain 原 TiKV 适配器没有该调用，而 /metrics 使用 Prometheus 默认 registry。
这构成确定的监控缺口，不等于已经定位 Put 慢的根因。

新增 `pkg/storage/tikv/metrics.go`，在包初始化时调用客户端 RegisterMetrics 一次，
不随池中 client 数或存储实例数重复注册，也不重新初始化已有 observer。
新增测试观察客户端事务/Get、退避和请求 histogram，并从实际 DefaultGatherer
核对导出。临时停用注册函数时测试 exit 1（缺失 backoff 指标），恢复 init 后通过。
完整适配器包 race 通过（1.693s），随后 vet 通过；真实 TiKV 环境依赖测试仍按原条件
跳过，不能作为集群端到端验证。提交前 inventory 已通过，仍为 720 项、174/197/184/165。
产品改动尚未提交或发布；四分片及提交后门禁尚待完成。当前线上继续使用回滚版本。

### 826fd14c 指标注册修复：提交前后门禁终态

上述待提交状态已被本节取代。产品提交为
`826fd14cb5854b9d0da5a8cdc155fdcc9b49b36b`，仅增加指标注册及回归测试两个文件。
提交前 inventory 与四分片全部 exit 0：174/197/184/165 项，耗时分别为
478.714/542.370/391.175/768.636s。提交后立即复验 inventory，四分片也全部 exit 0，
耗时 480.462/541.561/392.339/768.016s；两轮各 720 项，未缩减测试范围。
提交后完整 TiKV 适配器 race 通过（1.760s），vet exit 0；依赖外部 TiKV 的测试仍按
原环境条件跳过，不能替代真实集群验收。

私有证据：`security-client-metrics-code.sha256`、
`security-client-metrics-pre-complete.json`、`security-client-metrics-post-complete.json`，
以及同前缀的 pre/post inventory、shard-0/1/2/3 和 post-package-race/vet 日志。
源码摘要在提交前及提交后核对一致。只读 etcd 基线仍为
`5cd9f4ee13801e18825d661e5005ae599460bc3a`，工作树干净；其 server 指标同样在 init
注册并通过默认 promhttp handler 导出，但 TiKV 指标本身不是 etcd 协议兼容性证明。

另一次固定 kubeconfig/context 的只读 Pod 快照保存在
`security-client-metrics-baseline-pods.json`：KubeBrain/PD/TiKV 各三个副本 Ready，
各容器 restartCount=0。此证据仅代表采样时点，不替代连续后端健康及后续发布前检查。
下一步推送后核对新 CI 的完整 headSha、镜像摘要和实际导出；旧 34057ee8 升级入口
绑定旧源码，不可直接复用。原 900s 超时失败继续保持开放，不以指标修复宣称关闭。

### 4f2d2402 发布进行中及新验证入口

文档后继 `4f2d24020d8121c888a978f6d905e87e29b2d439` 已推送 dbaas，
触发 CI [34407926007](https://github.com/fivetime/kubebrain/actions/runs/34407926007)。
已由 Runner 接走，最近查询运行到 Verify automatic target architecture；尚无成功发布终态。
同一 watch 进程句柄 42319，私有日志 `security-client-metrics-4f2d2402-ci-watch.log`。
运行状态更新暂不 push，以免打断当前构建。

私有 `run-prepull-4f2d2402-upgrade.sh` 绑定上述完整源码、产品 826fd14c、CI ID
及 `security-client-metrics-post-complete.json`，仍须待实际镜像回执才能继续。
新工具目录 `client-metrics-4f2d2402-tools` 包含重编译的 image-prepull、uid-delete
及固定 kubeconfig 包装器；两个二进制 buildinfo 均为 Go 1.26.8、上述完整源码、
vcs.modified=false。没有扩大旧工具的源码差异豁免；工具及入口摘要分别保存在
`security-client-metrics-4f2d2402-tools.sha256` 和 `security-client-metrics-4f2d2402-entry.sha256`。

入口 bash -n 与固定摘要 ShellCheck 镜像检查通过，后者仅只读挂载两个脚本、禁用网络，
未挂载 kubeconfig。缺少镜像回执的负例 exit 1，明确输出
`verified candidate image receipt is absent; no cluster access performed`。
日志为 `security-client-metrics-4f2d2402-entry-{shellcheck,missing-image}.log`。
未创建占位成功回执，未执行部署；6000 次/100ms 间隔及原 SLO、900s 完成窗口仍未更改。

后续同一 CI 查询已进入 Build and push TiKV test image，前序隔离准备合同检查成功；
尚未取得发布终态。新增私有只读采样入口 `capture-client-metrics-4f2d2402.sh`：
要求真实镜像回执及固定 namespace UID，逐个检查候选 Pod image、Ready、UID、containerID、
imageID、restartCount 和启动时间，在采样前后核对身份相等；探针同样做前后身份核对。
从探针用固定 info CA/serverName、connect-to 目标 Pod IP 获取 /metrics，保持 TLS 校验，
每次命令有外层 20s 超时、curl 10s 超时、4MiB+1 有界捕获，空响应或超限拒绝。
每份样本要求 request_seconds、txn_cmd_duration_seconds、backoff_seconds 的客户端
count 序列存在；只输出 diagnostic_only，不产生部署验收成功回执。不同 Pod 的采样是
顺序执行，不是全局原子快照；后续增量分析仍须检查同一 Pod 进程启动时间与计数单调。

该入口 bash -n、固定摘要 ShellCheck 通过；缺少镜像回执时 exit 1 且在集群访问前停止，
日志 `security-client-metrics-4f2d2402-capture-{shellcheck,missing-image}.log`。
尚未运行真实采样，也未使用占位回执测试可执行路径；实际采样能力仍待发布后的证据。

### 4f2d2402 发布成功与只读部署前检查启动

CI 34407926007 于 **2026-09-09T22:08:00Z** 全部成功结束，watch 42319 exit 0，
包括双平台发布验证、dbaas promotion 及清理步骤；Node.js 20 弃用警告保留。
独立取得的原始索引 SHA-256 与 registry 摘要一致：
`sha256:0f16988bc838506cb42652aa52f6e7077142e845e4f707c8aa5788975bcd0dce`；
amd64 为 `sha256:05928200d84c718612844a06bb958d8fa251d898cc44bd8107025a71fdf7a58a`，
arm64 为 `sha256:4c78f29b524040ffcddc9770d8a668d0dea62f45ea439e1d1071519b0f1b58fd`。
本地新 image-prepull 的 verify-release 通过；独立读取 dbaas 标签也指向同一索引。

按 amd64 摘要拉取成功（62209 exit 0），本地 network-none/read-only/drop-capabilities
运行实际镜像 version：源码 4f2d2402 全 SHA、版本 0.0.0-dbaas-4f2d24020d81、
Go 1.26.8、linux/amd64、BuildTime 21:37:52Z。OCI 标签匹配，运行用户 65532:65532。
提取主程序的 Go buildinfo 核对 fork 模块版本与 go.mod 一致，sum 为
`h1:xmTt2n1e/Yy8Tq5cCn4MqQsTtzTQWMuKX2GAqDkJuJg=`；临时提取容器已删除。
实际成功镜像回执 `security-prepull-4f2d2402-image-verified.json` 已建立，权限 0600，
明确仅证明发布身份、不证明部署验收。相关证据均为 security-client-metrics-4f2d2402 前缀。

新入口 verify 已启动，句柄 13790，日志 `security-client-metrics-4f2d2402-entry-verify.log`，
证据目录 `prepull-4f2d2402-verify.wqqpzMli3nsU`。仍为只读检查，尚未启动 execute，
不以发布成功覆盖旧的升级完成超时失败。

随后同一 verify 13790 exit 0，输出 `CANDIDATE_UPGRADE_GUARDS_PASSED`，
源码及镜像索引与上述回执一致，`cluster_mutations=0`。尚未进行实际隔离准备或升级。

### 4f2d2402 准入通过与受控升级启动

新候选 check-admission exit 0，输出 `PREPULL_ADMISSION_CONFIRMED dryRun=All containersExecuted=false`。
全部 Job/Pod 名称+UID 的 before/after 投影 cmp exit 0，未创建实际容器；
日志 `security-client-metrics-4f2d2402-admission.log`，journal `admission-4f2d2402-01`。
hold 11618s、minimum remaining 10958s，与原 runner 预算一致。

随后仅启动一次新入口 execute，进程句柄 **5877**，日志
`security-client-metrics-4f2d2402-upgrade.log`，证据目录
`prepull-4f2d2402-execute.EnZ8F8j9cvg3`，恢复 journal `prepull.zPmp0ZJqqAuy/attempt`。
原 6000 次/100ms、public 5s/direct 30s、900s 完成窗口均未调整；本次取得客户端
阶段指标是诊断目的，不能预设升级验收通过。失败仍须跟踪原流程回滚与 UID 范围清理终态。

当前已取得 `PREPULL_READY`，holder nonce `e65be03c844ad8a673c2bd636469c7d8`，
三个 Pod 分别为 -00-sq8p2/-01-hqztv/-02-f89df，UID 分别为
fd3af2fe-6a4e-4e2d-aef4-18af6385fb7b、dd09f944-9a49-4fa7-b5c7-31928e49c6ea、
5bd03a97-1e22-41ef-a18d-1136be95313e，放置在 worker1/2/3。
流程仍运行，正在准备探针，尚无完整升级或指标采样结论。不要重复 execute。

### 4f2d2402 早期 direct watch 失败及恢复终态

原 execute 5877 已 **exit 1**，不再运行。首次 runner 失败为
`availability probe failed during rollout`，探针最终报告
`PROBE_FAIL iteration=116: direct watch recovery exceeded 28.07110831s: context deadline exceeded`。
这是滚动期间的早期恢复失败，不是此前 900s 完成窗口超时，也没有完整成功 summary。
实际采样入口未能执行，新客户端耗时样本尚未取得。

证据 `security-client-metrics-4f2d2402-events.json` 与升级日志显示：
旧 kubebrain-2（UID c9c25ac0-7b49-4b4e-8587-3c7452100270）22:17:12Z Killing；
候选 UID 16403130-ff9b-4317-b13d-76299a1d761a 在 22:17:55Z 因两个 generic ephemeral
PVC 仍属旧 Pod/正在删除出现 FailedBinding、FailedScheduling，22:17:56Z 调度并报告
两卷 attach 成功，直到 22:18:08Z 才 Pulled/Created/Started。Pulled 明确为镜像已缓存，
不是业务 Pod 重新下载镜像。22:18:07.442Z 探针已输出 final 进度（仅完成 115 次），
早于候选容器启动；随后客户端日志含 direct Pod DNS no such host。
22:18:17Z 候选 Killing 属回滚阶段，不是首次失败原因。

这些事件支持优先检查 Pod 替换、旧卷清理及新卷挂载/容器创建的整体间隔，不能把所有
延迟归因于 PVC 创建（调度阻塞事件仅约一秒），也不能归因于新指标注册或候选启动后的
业务逻辑。28.071s 是该阶段剩余等待耗时，原 direct 恢复上限仍为 30s，未改门限。

执行日志末尾已有 fixture keys/users/roles/leases 全零及 `PREPULL_CLEANUP_CONFIRMED`。
新鲜只读查询确认 StatefulSet generation/observed 9/9、Ready/updated 3/3，
current/update 均为 kubebrain-696c87f8f9，回到原 a245c95f 完整镜像；名称含 prepull
的 Job/Pod 集合为空。首轮复核 updated=2 为中间状态，已被本次终态取代。
未重跑 upgrade，也未降低负载或超时标准；旧 900s 完成超时与本次 Pod 替换恢复失败均保持开放。

### 退出路径复核：证据与待验证假设

本次 source.json 确认实际 preStop 为 sleep 25 后 POST /drain（curl 上限 10s），
terminationGracePeriodSeconds=45，两个 4Gi rook-ceph 工作卷均为 generic ephemeral。
源码 cmd/main.go 的 SIGTERM 后进程强制退出上限为 15s；preStop 属于信号前阶段，
这些预算不能直接相加解释实际耗时，也不能将 Killing 事件等同于进程已经退出。

退出代码依次等待 endpoint transport、server、backend workers、checkpoint release 和
storage close。实际参数 --tikv-client-num=16，pkg/storage/tikv/tikv.go 的 closeClient
逐个关闭池内客户端；fork KVStore.Close 会先 cancel 并等待自己的后台任务，然后关闭
oracle、PD、lock resolver、RPC 和 Region cache 等。由此得到一个可验证假设：串行关闭
可能累积等待时间，但当前缺少已删除旧 Pod 的完整退出日志/最终容器状态，不能声称它
造成了此次超时，也不能把剩余时间全部归因于 CSI 卸载。

后续诊断应在任何新的受控替换之前开始保存旧 Pod 的日志及 UID 对应的容器终止状态，
区分 /drain、SIGTERM、shutdown complete/force exit、卷卸载及新容器启动。没有为验证
假设重启 worker/PD/TiKV，没有把工作卷改成 node-root emptyDir，也没有缩短保护预算。

### 按 Pod 身份绑定的退出状态观察器

新增私有 `observe-kubebrain-2-exit.sh EXPECTED_POD_UID SECONDS`，只读固定测试
namespace/kubeconfig/context 和 kubebrain-2，启动前核对 namespace UID 与 Pod UID。
观察时长限制 5..900s，kubectl watch 由外层 timeout 收敛，投影仅含事件时间、UID、
resourceVersion、删除时间、conditions、containerID、restartCount、state/lastState，
不保存 Pod 环境变量或 Secret。跨同名 Pod 替换保留不同 UID，不把后继当作旧进程。
输出上限 32MiB；分别记录 watch/解析/捕获退出码，空或超限数据拒绝，原始片段保留。
结束时再次验证 namespace UID，并明确 termination_not_assumed=true。

bash -n 与固定摘要 ShellCheck 通过；对当前 UID
69fb75b9-6fde-49e7-b0e7-878a625e1e4e 执行 5s 无变更试运行，exit 0，
证据目录 `pod-exit-observation.FMFv4JArYe03`：watch_exit=124（预期到时停止），
parse_exit=0、capture_exit=0，实际 ADDED 事件为该 UID 的 Running/Ready 状态。
错误 UID 负例在观察开始前拒绝，exit 1。此测试只证明采样通路和边界，尚未观察真实退出。
后续受控替换还需同时启动该旧 UID 对应的有界日志跟随，才能区分正常 shutdown 与强制退出；
本轮未执行新升级或删除 Pod。

### 带旧进程观察的第二次 4f2d2402 诊断运行

在确认上轮已终态且回滚完成后，启动状态观察 61217（900s，上限到时停止），目录
`pod-exit-observation.cod2n83PDDjv`，以及旧 Pod 日志跟随 16333（900s/32MiB 上限），
文件 `security-4f2d2402-retry-old-pod.log`。二者均在替换前取得旧 UID
69fb75b9-6fde-49e7-b0e7-878a625e1e4e 的实际数据，再次核对 UID 未改变后，启动一次
有观察的新 execute 26159。日志 `security-client-metrics-4f2d2402-upgrade-observed-retry.log`，
证据目录 `prepull-4f2d2402-execute.zKkaHEtrPEq6`，恢复 journal `prepull.n0QMNi5yVFvq/attempt`。
本轮目的为补齐退出因果证据，非重新证明冷拉取；负载、超时及部署参数均未调整。
已取得 PREPULL_READY/VERIFIED，execute 仍运行，不得重复启动。

关键新证据：22:33:16.401Z 日志记录 info TLS/root port 8080 shutdown，随后在
22:33:31.401Z 明确 `force exit due to graceful exit timeout`。Pod watch 中同一旧 UID
的 terminated.finishedAt=22:33:31Z、exitCode=1、reason=Error，22:33:32Z DELETED。
日志跟随 16333 已 exit 0，log_exit=0/capture_exit=0。这证实进程耗尽了 15s 退出窗口，
不是仅由卷创建耗时推测。当前只见 info 端口完成，无 client/peer root shutdown 或
shutdown complete；结合 Endpoint.Run 在全部端口退出后才进入 server/backend Close，
应优先定位传输层等待，而不是先修改 TiKV 客户端串行关闭。具体阻塞位置尚待复现证明。

随后同一 execute 26159 已 exit 1，`PROBE_FAIL iteration=389: direct watch recovery
exceeded 28.093769862s: context deadline exceeded`，fixture 全零及 PREPULL_CLEANUP_CONFIRMED
已输出。最新 StatefulSet generation/observed=11/11、Ready=3，但 updatedReplicas 尚缺，
不可仅凭 Ready 宣称回滚终态；继续核对实际 Pod 镜像。状态观察 61217 仍按原 900s 上限运行。

后续回滚终态已确认：三个实际 Pod 全部为原 a245c95f 镜像、无 deletionTimestamp、Ready；
StatefulSet generation/observed=11/11、Ready/updated=3/3、current/update 均为旧 revision。

### 本地复现 TLS quiesce 最终关闭循环

检查 pkg/endpoint/security.go 发现 secureServer.serve 为内部 cmux runners 创建独立
Background context，只在 Serve 返回的 defer 取消；而 runSubServer 对成功 quiesce
会等待 ctx.Done 后再返回。secureServer.close 原先只关闭内部服务、未取消该 context，
造成“等待 Serve 结束才取消，而 Serve 等待取消才结束”的生命周期循环。

新增真实 loopback TLS 测试 TestSecureQuiescedServerFinalCloseReturns，先确认实际 HTTPS
请求可用，再 quiesce，证明 quiesce 本身不结束服务，最后要求最终 Close 释放 Serve。
首轮测试夹具误传 nil identity registry 导致 panic，修正夹具后取得有效 RED：1.143s，
明确失败于 final TLS close did not release quiesced inner runners（并非证书或启动失败）。

待提交修复让最终 close 在加入内部服务之前取消包装层 context，用 mutex 保护
close/serve 交错；close 先发生时，后续 serve 不再启动内部 runners。补充 close-before-serve
与重复 close 测试。三个相关测试（含原错误聚合测试）race/count=10 通过，2.237s。
尚未完成整个 endpoint 包及提交前后生产门禁，未提交/推送/部署；不据此宣称线上失败关闭。

随后完整 endpoint race 第一轮通过（15.470s）。进一步补充原生 gRPC/TLS 的实际 Health
Watch：quiesce 必须保留已接入长流，最终 Close 必须在有界窗口内终止长流并释放包装层；
补充 20 次 Serve/Close 交错测试。所有 TestSecure* 在 race/count=10 下通过（2.921s）。
接下来冻结代码，对包含新覆盖的 endpoint 全包再次复验，并执行提交前四分片；仍未提交。

### TLS 最终关闭修复：本地门禁完成，准备恢复 CI

产品提交 `12b4aedc6c23f5a958e94779177b10917ac49bd4` 已完成提交前后门禁：
两次 inventory 均确认 720 项、四分片 174/197/184/165，所有分片 exit 0。
提交前耗时依次 474.376/539.381/394.116/770.466s，提交后
491.440/556.584/393.676/785.897s；扩展覆盖后的 endpoint 全包 race
提交前 15.593s、提交后 15.758s，vet 均 exit 0。源码校验和复核一致。
私有回执为 `security-tls-final-close-{pre,post}-complete.json`，日志使用同名前缀。
仅本地验证完成，未推送或部署；不能据此认定实际升级失败已关闭。

补齐上一轮观察终态：状态观察 61217 已 exit 0，所有旧观察/升级进程均已结束。
重读第二次 4f2 升级完整日志，第 8–10 行已有 `CRITICAL: candidate rollback Pod runtime
identity mismatch`，所以执行器并未把当时尚未收敛的实际 Pod 状态误报为回滚成功。
后续独立查询才确认最终恢复；本轮新鲜只读查询再次取得 StatefulSet 原 UID、
generation/observed 11/11、Ready/updated 3/3，模板仍为 a245c95f 原镜像。
若以后改善回滚等待，应保留实际 Pod 身份校验，不将这次诊断误写成“缺少校验”。

用户通知 Runner 机器恢复后，已核对上一轮 CI 34407926007 全部成功；仓库 Runner
查询暂返回 total_count=0，尚不能确认新任务接单。下一步推送本修复及 docs-only 交接记录，
以新 run 的 headSha、排队/接单结果确认。旧服务仍未包含本 TLS 修复，首次替换旧 Pod
仍可能遇到旧退出缺陷；不降低现有 SLO，不把预热镜像重跑视为完整冷升级证明。

后续已将 docs-only 后继 `dd339bc1f9586fb77b5743970aea99765012f23c` 推送 dbaas，
触发 CI [34417282642](https://github.com/fivetime/kubebrain/actions/runs/34417282642)。
Runner `raas-1567` 已 online/busy，job 102684841419 于 23:31:47Z 开始，
不再处于等待 Runner 注册状态；CI 尚在执行，不代表已发布镜像。

私有新入口 `run-prepull-dd339bc1-upgrade.sh` 已绑定该 source、产品 12b4aedc、
CI run 及 TLS 提交前后实际回执，保留既有 SLO/负载/资源身份约束。
默认 verify；尚无实际镜像回执时负向测试 exit 1，明确在集群访问前停止。
对应 `tls-final-close-dd339bc1-tools` 下两个恢复工具已重新编译，Go 1.26.8、
vcs.revision=dd339bc1 完整 SHA、vcs.modified=false，固定 kubeconfig wrapper
继续拒绝调用方覆盖；工具校验和为 `security-tls-final-close-dd339bc1-tools.sha256`。
入口和 wrapper 的固定版本 ShellCheck exit 0。以上仅本地准备，未创建集群资源或部署。

指标诊断入口 `capture-client-metrics-dd339bc1.sh` 也已绑定新 source/run/探针名；
bash 语法与固定 ShellCheck 均通过，缺少真实镜像回执时 exit 1、集群访问前停止。
尚未实际采样。原 `observe-kubebrain-2-exit.sh` 校验和仍匹配，可在升级前以新鲜 Pod UID
启动有界观察；目前未启动观察。CI watch 会话 66765 已启动，日志
`security-tls-final-close-dd339bc1-ci-watch.log`，不得因一次观察超时重复触发构建。

CI 仍执行时，已从候选 tag 独立读取实际 OCI 索引：
`sha256:0ce85e66320b27bf4cdb8f11981a76cba58926fa0e835fc47dc663f709b588ce`；
amd64 为 `sha256:bb11ffc594b2a5e6153ad2fb44174355576415a91a045da9924f0a2da84e762b`，
arm64 为 `sha256:7c87416101097064adbb8c59fc678e41e4b15aa16a9edba508aad18fc277018d`。
本地 exact-source verify-release exit 0；原始索引及结构验证输出使用
`security-tls-final-close-dd339bc1-{index,release-identity}-observed.json`。
按该不可变索引启动本机 amd64 拉取会话 29672，日志同前缀 `amd64-pull.log`。
这只是候选字节观察，CI 成功、实际版本/模块与最终 promotion 仍待验证，未创建成功镜像回执。

随后本机 amd64 拉取 29672 exit 0；隔离运行 `--version` 会话 66613 exit 0，
实际报告 source dd339bc1 完整 SHA、版本 0.0.0-dbaas-dd339bc1f958、Go 1.26.8
linux/amd64、构建时间 23:31:57Z，与 OCI labels 相符，镜像默认用户 65532:65532。
从未启动的临时容器提取主二进制，buildinfo 确认实际替换为
`github.com/fivetime/tikv-client-go/v2 v2.0.8-0.20260909023231-832b70fd622f`，
sum `h1:xmTt2n1e/Yy8Tq5cCn4MqQsTtzTQWMuKX2GAqDkJuJg=` 与 go.sum 一致。
临时提取容器已删除，二进制/buildinfo/config/version 证据仍保存在私有同前缀文件中。
CI 与最终 promotion 尚未完成验证，仍不创建成功镜像回执或启动部署。

2026-09-10 00:03:33Z，CI 34417282642 全部成功（含八项必要发布步骤及清理），
watch 66765 exit 0；发布后独立读取 dbaas 标签索引，SHA256 与候选 0ce85e66 完整摘要一致。
已基于实际 CI/索引/版本/模块证据创建私有 0600 回执
`security-prepull-dd339bc1-image-verified.json`，范围仅 published_image_identity_only。
完整 CI 结果在 `security-tls-final-close-dd339bc1-ci-completed.json`。
随后启动只读入口 verify 会话 93413，日志同前缀 `preflight.log`；未 execute，
尚未创建 holder/探针或切换业务镜像。检查正在进行，需取得真实终态后才能继续。

只读 verify 93413 随后 exit 0，输出 CANDIDATE_UPGRADE_GUARDS_PASSED、cluster_mutations=0，
证据目录 `prepull-dd339bc1-verify.NE2CEFkxuDGR`。镜像、恢复工具、固定集群身份、
rook-ceph 数据卷与后端健康前置检查通过；实际隔离准备/受控升级尚未开始。

随后新鲜核验三个旧 Pod 均 Ready、无删除时间且实际 spec 为 a245c95f 镜像；
旧 kubebrain-2 UID 为 `0e0da27b-a868-4150-9c89-c9c5f50f478a`。
绑定该 UID 启动 900s 状态观察 59652，目录 `pod-exit-observation.83ufQrAu3Nd0`，
同范围日志跟随 95605（900s/32MiB+1 捕获上限，完整性仍需结束时核验）。
再次确认旧 UID 不变后启动 execute 85909，日志
`security-tls-final-close-dd339bc1-upgrade.log`，证据目录
`prepull-dd339bc1-execute.irUdLRRi5HBM`。当前执行中，不得重复启动或先行宣称通过。
SLO、6000 次负载、900s 完成窗口及其他部署配置不变；先隔离镜像准备，再按入口受控升级。

本轮 execute 85909 已 exit 1：PREPULL_READY/VERIFIED 后探针启动，首个失败为
`availability probe failed during rollout`，`PROBE_FAIL iteration=117: direct watch recovery
exceeded 27.986725732s: context deadline exceeded`。fixture 全零和 PREPULL_CLEANUP_CONFIRMED
已输出；回滚早期日志明确 CRITICAL kubebrain-2 runtime identity mismatch，并非回滚成功。
后续独立核验终态 generation/observed 13/13、Ready/updated 3/3、current/update 均为
kubebrain-696c87f8f9，三个实际 Pod 全部旧 a245c95f 镜像、Ready、无 deletionTimestamp。
恢复后的 kubebrain-2 UID 为 `8d3f5d17-5296-4df7-9eb5-2eb5b30c0bf9`。

旧日志跟随 95605 exit 0：00:09:25.331Z 仅 info root port 8080 shutdown，
00:09:40.331Z 再次 force exit due to graceful exit timeout。旧实例未包含 TLS 修复。
事件证据同前缀 `events.json`：候选 UID `9fbcf1ed-cea6-4e1c-b82e-1f563ce2dfcf`
00:09:41Z 完成调度，00:09:42Z 卷 attach，00:09:53Z 使用已缓存镜像启动；
00:10:01Z 因回滚 Killing，00:10:28Z 已出现恢复 Pod，00:10:34Z 原镜像启动。
不能把升级失败声明为已修复，也不能从事件间隔单独证明候选进程优雅退出；
状态观察 59652 仍在原 900s 窗口运行，需继续检查同 UID 的实际终止状态。

已从实时状态记录补齐终止证据：旧 UID exitCode=1/Error、finishedAt=00:09:40Z；
候选 UID exitCode=0/Completed、finishedAt=00:10:28Z，restartCount=0。
因此本次候选在回滚时确实正常退出，与旧实例强制失败退出不同；这项局部证据
不等于整轮升级通过，也不覆盖有长期已接入 watch 的全部退出场景。
旧日志 12157 字节，未触达 32MiB 截断上限。仍未取得三个候选实例的客户端指标采样。

进一步核对探针源码：directRemaining 从该次 Put 起算的 30s deadline 扣除已消耗时间，
报错 27.9867s 是剩余 direct wait 预算，不是另设更低门限。final progress 在 run 返回时
defer 输出，00:09:51.742Z 已记录 completed=116/final=true，早于候选容器
00:09:53Z Started；00:09:58Z 最终 PROBE_FAIL 打印包含了客户端清理等待，不能误作
最初故障时刻。状态观察候选首次 Ready=True 为 00:10:01Z。

下一步决策边界：新镜像无法回溯修复正在退出的旧进程。若先通过维护迁移建立含修复的
三副本基线，再按原 SLO 验证新基线滚动，需要明确区分“安装修复”与“通过升级验收”；
不能放宽探针后将其记为原 SLO 通过。当前尚未执行这种不同验收语义的迁移，也未重试。

原 900s 状态观察 59652 已自然结束，exit 0，输出 POD_STATUS_OBSERVATION_FINISHED。
同目录 result.txt 为 watch_exit=124、parse_exit=0、capture_exit=0，表示有界 watch
到时结束且解析/捕获成功，并非升级超时。旧/候选终止记录已保存；所有本轮执行与观察
会话均已终止。最新只读 StatefulSet 核验仍为 generation/observed 13/13、Ready/updated 3/3。
维护迁移尚待用户明确确认；自动 goal 续行不作为维护迁移批准，不重新部署或更改验收门限。

### 用户批准一次维护迁移（2026-09-10）

用户明确确认该测试集群没有业务、专用于本项目，并同意维护期间短暂连接中断。
据此建立私有独立入口 `run-maintenance-dd339bc1.sh`，仍绑定已验证 dd339bc1 镜像、
原集群/存储身份和恢复工具，不修改正式验收入口或产品默认配置。
本次维护 direct recovery 预算为 120s、完成窗口 1800s；后者容纳此前已观察到的
6000 次串行写入加 pacing 耗时，不将其视为原 900s 完成验收通过。
保留 6000 次、public 5s、数据/租约/流校验、原镜像失败回滚和隔离 holder 清理。
入口显式输出 acceptance_30s=false；即使维护成功，也必须另按原门限完成正式验收。
语法和固定 ShellCheck 已通过，先执行只读 preflight，不因用户授权跳过镜像或后端检查。

维护 verify 55977 exit 0，证据 `maintenance-dd339bc1-verify.UhpQOOkCvOFI`。
新鲜旧 Pod2 UID 8d3f5d17-5296-4df7-9eb5-2eb5b30c0bf9 已绑定状态观察 71333，
900s 目录 `pod-exit-observation.FpVmLP3ubLi2`；随后 execute 83963 已启动，
证据 `maintenance-dd339bc1-execute.5bChtnFOUlbL`，恢复 journal `prepull.RB6uE9CG6E1m/attempt`。
日志 `security-maintenance-dd339bc1-execute.log` 已输出维护范围及 PREPULL_READY，仍在执行。
指标入口 `capture-maintenance-dd339bc1-metrics.sh` 仅将探针名绑定 maintenance-dd339bc1，
其余镜像和采样身份条件不变，语法与固定 ShellCheck 通过，尚未实际采样。
不得将进程仍运行当作安装完成，也不得重复 execute。

维护中间进展：PREPULL_VERIFIED 后三个实例已更新，StatefulSet generation/observed
14/14、Ready/updated 3/3。实际三个 Pod spec/imageID 均为 0ce85e66 完整候选索引，
Ready、restartCount=0、无 deletionTimestamp；Pod0 UID c42b7073-80fe-46b9-ae73-5d94324501b0，
Pod1 UID 288b0806-5cdb-4002-97a6-fa2cb5081e9e，Pod2 UID d0366f42-4aef-4529-89fd-12a24f4d1077。
02:21:08Z 探针 completed=720/6000，仍在执行，无完整维护通过结论，失败回滚仍有效。
已启动指标采样 38291，日志 `security-maintenance-dd339bc1-metrics-capture.log`。

指标采样 38291 已 exit 0，CLIENT_METRICS_CAPTURE_CONFIRMED；证据目录
`client-metrics-dd339bc1-sample.X69kCXjvFfyM`。三个实际候选实例均取得 request_seconds、
txn_cmd_duration_seconds、backoff_seconds 的 count family，采样前后 Pod/进程身份一致。
这是客户端指标注册修复的实际集群证据，不是升级 SLO 通过，也尚未完成延迟根因分析。

### 维护迁移完成及 scale-lab 整合收尾

维护 execute 83963 已 exit 0，02:39:37Z 前后完成清理，输出
MAINTENANCE_MIGRATION_FINISHED acceptance_30s=false。6000/6000 操作成功、fail=0，
public watch 6000、direct watch 6000x3，public/direct lease 均存活，range stream
1089、snapshot 1；fixture 全零及 PREPULL_CLEANUP_CONFIRMED。最大 public 延迟
1678ms、direct 延迟 29829ms、direct lease recovery 25447ms。尽管观察最大值低于
30s，本次配置仍为维护 120s/1800s，不冒充原 30s/900s 正式验收。旧状态观察 71333
也已 exit 0。本次收尾新鲜查询确认 generation/observed 14/14、Ready/updated 3/3，
服务镜像为已验证的 0ce85e66 完整索引，而非更早记录中的回滚镜像。

用户要求整合本机独立 scale-lab 后，产品/测试提交
`0b680bc3c46c3e655d9c873121db15481cdf607e` 已将 slowwatch/watchflood 纳入
仓库内 hack/scale-lab，统一本地构建/测试入口并保留更完整的仓库 loadgen。
旧 /root/kwok-scale-lab 已在完整私有归档与比对后删除，无旧路径软链接；
恢复路径及逐文件去向见 hack/scale-lab/docs/local-tools-migration.md。
提交前后各 720 项、inventory 和四分片全部通过：前 0/1/2/3 分片耗时
461.547/518.607/389.265/750.411s，后 464.054/526.357/391.705/755.837s。
scale-lab（含嵌套模块）及 build race/vet 均通过，十个文件校验和一致。
私有证据目录 /root/.local/state/scale-lab-consolidation-gates.MzmIyz4A；
原会话 20754/3658/22689/47325 均已 exit 0。整合未执行集群压测或部署。
下一步发布整合代码，并在修复版基线上继续原门限滚动复验及已有 Kubernetes/KWOK
测试的定向回归；不是重新从零开发已有测试，也不将历史大规模结果直接外推到本环境。

### 2026-09-10：整合发布成功，修复基线正式升级复验因完成期限失败

发布源码 `3b15bd16d27dc9e5be93a80802ac18b65c42479e`（产品提交 0b680bc3）
的 CI [34459431691](https://github.com/fivetime/kubebrain/actions/runs/34459431691)
于 09:46:21Z 全部成功，包含安全扫描、双架构构建、发布验证与 dbaas 标签提升。
独立验证候选和提升后索引均为
`sha256:a16fb0ad7cadbec31109e9ccd7198f5d86baf942524ade4ee04d1a45aa89f5fa`；
amd64 为 `sha256:9e886ebb23cb8ffa728985ccc9d8efe53c09fd0e4ec68add7e1bb2232fae2dbd`，
arm64 为 `sha256:70ecd50b44c0b244ab3c5ee46fca6f5d6161bab6989c992da9594ccf9bbdd828`。
实际 amd64 程序版本/Git SHA、Go 1.26.8 和 fork client-go 模块版本均已核验。
私有 `security-prepull-3b15bd16-image-verified.json` 仅证明发布身份，不证明部署验收。

只读预检 72864 exit 0；正式 execute 14062 最终 exit 1。入口
`run-prepull-3b15bd16-upgrade.sh` 保持 6000 次、0.1s 间隔、public 5s、direct 30s、
滚动完成后 900s 完成期限，不使用维护 120s/1800s。证据目录
`prepull-3b15bd16-execute.SCoemo4uNFJK`、预拉取 journal `prepull.95Tq9glNxRnB/attempt`，
主日志 `security-prepull-3b15bd16-execute.log` 均在既定私有状态目录下。
PREPULL_READY、PREPULL_VERIFIED 与 ROLLOUT_PROBE_COVERAGE_CONFIRMED 已取得；
候选曾为 generation/observed 15/15、Ready/updated 3/3、revision kubebrain-57fd8b6bbf。
随后明确输出 `availability probe did not complete within 900s` 并自动回滚。
09:59:33Z 诊断完成 1860/6000，10:09:29Z 完成 4680/6000；回滚期间仍曾推进到
5100/6000。后者不是截止时刻计数，也不是通过证据；本轮没有完整 PROBE_SUMMARY。
回滚期有界探针日志保存于 `security-prepull-3b15bd16-probe-during-rollback.log`。

终态新鲜核验 generation/observed 16/16、Ready/updated 3/3，current/update 均为
kubebrain-855b5bfb88；三个实际 Pod spec/imageID 均恢复维护修复版 0ce85e66 完整索引，
Running、Ready、restartCount=0、无 deletionTimestamp。Pod0/1/2 UID 分别为
f22feecb-1119-4e76-838e-5bf6eceec3fd、45257d09-42f5-4965-8cd3-3d1400804244、
a9402b10-8b4b-4627-a6a8-3b76e79dbcee。实际脱敏 Pod 证据
`security-prepull-3b15bd16-final-pods.json` 已通过镜像身份与本次临时 Pod 缺席检查。
主日志末尾 FIXTURE_CLEANUP_OK（keys/users/roles/leases 全零）及
PREPULL_CLEANUP_CONFIRMED。不能因回滚后 Ready 或此前维护通过而宣称本轮通过。

旧维护修复版 Pod2 UID d0366f42-4aef-4529-89fd-12a24f4d1077 在 09:53:05Z
实际 exit 0 Completed；同 UID 状态与三端口 root server shutdown、shutdown complete
日志互证。日志跟随 36875 exit 0、28575 bytes；观察 52747 exit 0，900s 到期 watch=124、
parse/capture=0、17037 bytes，目录 `pod-exit-observation.TRL7WUgiOOtF`。
这关闭本样本中的旧关闭循环现象，不外推为全部故障模式都能达标。

候选三副本指标采样 11453、61812 均 exit 0，目录分别为
`client-metrics-3b15bd16-sample.knfcNLn1or7w` 与 `client-metrics-3b15bd16-sample.IaYiGNIaBeZB`。
各次前后身份、两次跨样本进程身份一致，三个 TiKV 客户端 count family 均存在。
Pod1 的两次增量：896 次 Put RPC 平均 79.93ms、Put apply 72.45ms；1402 次批提交
平均 41.24ms、TiKV Commit RPC 18.96ms、Prewrite RPC 16.76ms。该 Pod 两次导出
backoff sum/count 均为零。批提交数量包含其他写操作，txn command commit 数量还含
只读事务；这些分母不同、部分层级重叠，不能相加推断 Put 关键路径，也不是 p99 或物理
fsync 耗时。计算与原始引用见私有 `security-prepull-3b15bd16-metrics-delta.md`。
下一步据此定位写路径成本及完成预算风险，不降低次数/间隔/SLO 掩盖失败。

已有真实 apiserver 回归脚本的只读准备发现：本机 kubebrain-dbaas-control-plane
kind 节点及所需 PKI 存在，但脚本默认节点名与此不同。脚本即使指定 APISERVER_BIN
仍从 kind 节点复制 PKI，清理还会回收相对基线新增的租约；不得与本轮探针或其他租约
创建任务并行执行。尚未启动本环境消费者回归，也未使用历史 scale-lab 默认 IP 部署。

### 配额读取批量化：本地提交前后验证通过，部署收益未验证

产品/测试提交 `260d51e17b9b4c3d99ee95978770645d4c299425` 优化 QuotaStatus：
启用配额且未固定存储快照时，利用已有 BatchGetter 一次读取 tracking、usage、alarm，
不缓存状态、不省略校验；保留解析顺序、缺失/损坏数据与存储错误处理。固定快照仍通过
InternalGet/GetAt，缺少批读能力时走原点读，禁用配额时仍只读取 alarm。
实际 StatefulSet 参数为 `--quota-backend-bytes=2147483648`，所以本环境使用该路径；
但该提交尚未部署，不能据此宣称 Put 延迟或 900s 完成期限已达标。

新增测试比较批读与点读的结果/错误，检查三物理键、一次 BatchGet/零独立 Get，
并验证固定 timestamp 不走普通 BatchGet。旧 HEAD quota.go 经 Go overlay 运行新测试
明确失败，说明测试能检出原逐键读取行为。初版测试包装器替换 live backend 的 kv
触发 fixture race，已改为无后台任务的独立状态对象；修正后后端配额 race 连续三次通过。
该初始失败不是产品竞态证据，原失败日志保留，不混作最终通过日志。

新增 BenchmarkBackendWriteStorageCalls 使用 memkv 统计存储 API 调用，逐次核验
revision 增量并校验最终值；不包含完整 RPC 的 auth/quota/TLS/代理/租约路径。
计数包装器保留 BatchGetter/Unwrap 能力，避免人为触发降级路径。每 100 次写入基准：
TxnApply 平均 4 次独立 Get、1 次 BatchGet、3 次事务内 Get、1 次提交；GetThenUpdate
独立 Get 为 6.01，其余相同，含起始缓存影响。早期未保留 BatchGetter 的 8/10.01
计数已作废。API 次数不等于网络 RPC 次数，内存耗时不外推为 TiKV 性能或集群 SLO。

本地证据目录 `/root/.local/state/kubebrain/quota-batch-gates.2cIdbgZb`：
提交前 runner 78422 exit 0，四分片 0/1/2/3 为 465.872/536.571/391.286/760.767s；
提交后 runner 80983 exit 0，为 470.558/529.794/391.099/761.062s。
前后 inventory 均为 720 项，分片数量 174/197/184/165，四分片退出码均为 0，
两阶段分别有 QUOTA_BATCH_PRE_COMMIT_GATES_PASSED 和 QUOTA_BATCH_POST_COMMIT_GATES_PASSED。
提交前完整 backend 包通过（50.236s），RPC quota race 三次通过（5.874s）；
提交后 backend quota race（8.515s）、RPC quota race（5.898s）、基准 race 三次
（1.918s）及 backend/server vet 通过。三个源码文件前后校验和相同，工作树干净。
这些是本地门禁，不是新镜像身份、真实消费者或在线升级验收；下一步发布后测量实际收益。
