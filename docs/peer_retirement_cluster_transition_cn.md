# 专用集群 peer 身份迁移与恢复准备

状态：第一阶段“旧叶证书＋双 CA”已在现场扩展、验证并恢复；独立新叶证书
和实验交接协议尚未部署。本页不表示新镜像或原 30 秒故障验收已经通过。
目标仅为 kubebrain-dbaas-test/kubebrain-local，
不涉及旧 Ceph 实例、TiKV/PD 数据卷或 TopoLVM VG 初始化。

## 成员私钥挂载

[挂载片段](../deploy/test-cluster/peer-retirement-mounts.patch.json) 是战略合并
patch 的离线输入，不是可直接 apply 的完整资源，也不是完整迁移操作。
它只替换 peer-tls Secret 来源与该卷的容器挂载，保留原 image、args、
service/成员身份、client/info TLS、资源配置、选举参数和所有数据卷。
策略文件此时只是随卷提供；没有添加启用实验功能的参数。

Secret 的十二个条目按三个 Pod 名分成三个目录；每目录只含该成员的
tls.crt、tls.key、ca.crt、policy.json，不包括 CA 私钥。容器只挂载
`subPathExpr: $(POD_NAME)` 到原 peer-tls 路径，POD_NAME 由 Downward API 的
metadata.name 提供，readonly，文件模式 0440。不存在完整卷根目录的容器
挂载，也不新增 init/sidecar 容器或 ServiceAccount token。

这是容器挂载范围约束，不是对被攻陷宿主机、kubelet 或有 Secret 读取权限
管理员的保密保证；底层 Secret 对象仍包含三把成员 key。生产长期方案须
另行评估逐 Pod 证书签发及 Secret 权限模型，不能据此声称有跨节点强隔离。

Secret 名 `kubebrain-local-peer-retirement-v1` 是准备阶段候选名。未来必须
新建不可变、有 owner 记录的版本化 Secret，名字冲突即停，不 apply 覆盖。
不得将 bundle 根目录直接打包、输出私钥到日志或把 ca.key 放入 Secret。

Kubernetes 官方说明 [subPathExpr](https://v1-36.docs.kubernetes.io/docs/concepts/storage/volumes/#using-subpath-with-expanded-environment-variables)
可使用环境变量展开子目录；[Secret 子路径挂载不自动接收更新](https://kubernetes.io/docs/concepts/configuration/secret/)。
因此本方案采用版本化材料与受控 Pod 重建，不能用于证明现有 Secret 投影
的在线证书热轮换。旧子路径材料留在运行 Pod 中时，单纯修改 Secret 不够。

## 迁移门限与阶段

执行前必须重新读取并保存实际 namespace/StatefulSet UID、resourceVersion、
generation、完整原 spec、容器镜像摘要、Pod UID、证书有效期和专用 owner。
离线片段自身没有并发前置条件，禁止盲目直接 patch。未来执行器必须将
合并结果转为带 UID/resourceVersion/旧 spec 校验的更新；先 server dry-run，
再逐阶段确认，不以仓库初始 manifest 替代当前 generation 38 的恢复快照。

1. 精确源码的镜像及测试 CI 通过、发布摘要校验完成。复核新叶证书剩余有效
   期和独立 SPKI、实际 backend scope；不得使用已过期的本地准备材料。
2. 保持旧 peer 叶证书和实验功能关闭，先让全部成员信任“旧 CA＋新 CA”。
   三成员新 Pod 和正常转发均验证后，才改变叶证书。不能一步把旧共享 CA
   换成新 CA，否则滚动期间新旧成员会互相拒绝。
3. 保持双 CA 信任，逐成员切换独立新叶证书和隔离目录，实际握手核对成员
   SPKI 与政策匹配，并确认每个 Pod 只能看到自己的文件。独立密钥都生效
   前不得启用 holder pin 交接协议。
4. 在新镜像上显式增加实验配置文件参数，验证真实 scope 绑定、普通转发和
   控制路由鉴权。全部成员切换完成后才单独评估移除旧 peer CA，分开记录
   这两个动作。严禁放宽证书验证来跳过过渡步骤。
5. 按原负载和 30 秒门限执行专用故障实验；保留原公共流和原始请求证据。
   rollout/Ready/正常读写通过不替代故障验收，默认 2PC 与 30s/25s/500ms
   选举配置不变。实验结束执行恢复，并重新验证。

以上阶段每次 Pod 重建都可能生成新的 Retain 临时卷；必须按本次 owner
记录 PV/PVC UID，只回收明确属于实验的临时卷，保留现有受保护卷。

## 逆向恢复不能直接跳回单 CA

先关闭实验配置入口（旧固定镜像不认识新增参数），保持当前 peer 正常通信。
若旧 CA 已移除，先在全部成员恢复双 CA 信任；然后在双 CA 下逐成员恢复旧
共享叶证书。全部成员都恢复旧证书后，才能恢复原单 CA 挂载和原固定镜像/
spec。直接让旧单 CA Pod 与新 CA 叶证书 Pod 混跑不是可靠回退。

各阶段都要核对实际证书和 UID，不把重试成功覆盖最初失败记录。完成恢复
前保留原 Secret 和本次版本化 Secret；确认无任何 Pod 引用且原实例验收
完成后，才可按精确身份清理本次对象。当前尚未创建这些集群对象。

## 已有验证与仍缺证据

契约测试使用 Kubernetes StrategicMergePatch 对仓库真实 StatefulSet
做合并，逐字段比较只允许的 Secret/mount 变化，并检查 POD_NAME 来源、
无额外容器/根卷挂载/CA key。它不证明 kubelet 运行时隔离、访问权限、TLS
信任过渡、现场 rollout 或故障门限；这些须在受控部署阶段分别验证。

## 现场 dry-run 与三节点假材料挂载 smoke（2026-09-18）

本轮保存实际 StatefulSet before.json，确认固定原镜像、UID、generation 38
及 3/3 Ready 后，将离线片段本地合并。实际 server dry-run 使用 JSON Patch
对 metadata.uid、metadata.resourceVersion、完整原 spec.template 三项先做
test，再替换候选模板。API Server 返回成功，候选 generation 39；**这个
generation 39 没有持久化**。再次读取的实际 spec 与原始对象完全相同，仍为
generation 38 和 3/3 Ready。

随后在独立临时 namespace `kb-peer-mount-smoke-zutq93pz` 创建不可变假材料
Secret、默认拒绝 ingress/egress 的 NetworkPolicy，以及三个短命验证 Pod。
Pod 分别在 worker1/2/3，使用现有已审核固定镜像的 /bin/sh，不启动 KubeBrain
服务，不使用真实证书/私钥、PVC 或 TiKV。安全上下文与实际实例一致：
UID/GID/fsGroup 65532、非 root、只读根文件系统、drop ALL、禁止提权，
不挂载 ServiceAccount token。每 Pod 使用正式片段的同一 Secret items
和 subPathExpr，假 tls.key 内容仅为其 Pod 名。

三个 Pod 全部 Succeeded、exitCode 0，记录了各自正确成员标识。验证：本机
tls.key 可读但不可写、内容对应 metadata.name、挂载目录恰好四个文件、
无 ca.key、无法通过挂载子目录或相邻目录读取其他成员路径。实际 Pod 只有
peer-tls 这一个 volume。它证明了本次三节点 kubelet 对这种布局的运行时
挂载行为；不证明真实 TLS 握手、权限模型抵抗宿主机入侵或信任迁移完成。

执行 86485 终态 0。清理前核对临时 namespace UID，删除该 namespace 及
本次 Pod/假 Secret/NetworkPolicy，随后确认 namespace 不存在。假材料可从
保留的 manifest 再生成；没有删除真实证书、数据卷或原实例资源。再次
读取实际 StatefulSet，UID、spec、generation 38、3/3 Ready 均与实验前一致。

证据在私有 owner `peer-retirement-preparation.QjFVoLV6/mount-dry-run.ZUTq93pz`：
before.json、candidate.json、preconditioned-patch.json、server-dry-run.json、
after.json、smoke-manifest.json、smoke-created.json、smoke-pods-final.json、
三个 Pod 日志、smoke-cleanup.log、after-smoke.json。一次性执行脚本有已执行
拒绝重用检查，不能将旧 resourceVersion 补丁直接用于以后真实变更。

## 三成员双向 TLS 过渡与回退演练

`TestThreePeerTrustTransitionAndRollback` 使用三个真实 SecurityConfig 的
server/client TLS 配置，经 net.Pipe 完成 mTLS 握手，不是仅解析证书文件。
起点与现有部署一样，共享旧 peer 叶证书；随后为每个成员提供不同的新 key
与叶证书。在每个单成员变更之后，检查其余所有成员组合的六个通信方向，
并核对双方实际看到的证书 serial，避免仅凭“某次握手成功”漏掉旧材料缓存。

测试保持同一组 TLS config，依次执行旧证书扩展双 CA、逐成员换独立新
证书、全部切换后删除旧 CA；逆向则先恢复双 CA，再逐成员恢复旧证书，
最后恢复单旧 CA。提前只换一个新叶证书，以及直接将一个成员退回旧单 CA
材料，均断言双向握手不能全部成功。删除旧 CA 后旧客户端证书遭服务端
拒绝；回退完成后新客户端证书也必须遭服务端拒绝，不能悄悄保留双信任。

这项测试演练信任顺序与普通 peer TLS 文件加载，不启用实验 holder pin
协议、不使用真实 TiKV，也不模拟 kubelet subPath 自动更新。现场方案仍需
按版本化挂载重建 Pod，并验证应用请求及原故障门限。

## 实际材料的三阶段准备与 dry-run

2026-09-18，私有 owner 的 trust-stages.Qx4iLtRM 已捕获实际 namespace、
StatefulSet 与原 peer Secret。原 Secret 的 UID/resourceVersion/data 在
准备前后保持一致；完整备份、解码旧 key 及候选 Secret JSON 都是 0600，
仅位于仓库外受限目录，不得输出 data 或加入 Git。

已离线生成三个不可变候选 Secret（**尚未创建到集群**）：

| 名称 | 叶证书 | 信任 |
| --- | --- | --- |
| kubebrain-local-peer-old-dual-qx4iltrm | 原共享 peer cert/key | 旧＋新 CA |
| kubebrain-local-peer-dual-qx4iltrm | 每成员独立新 cert/key | 旧＋新 CA |
| kubebrain-local-peer-new-qx4iltrm | 每成员独立新 cert/key | 新 CA |

第一个保留原三文件布局；后两个按成员提供十二个文件，匹配已验证的隔离
子目录挂载。都不含 CA 私钥。旧、新叶证书与 key 的公钥逐一相符，新成员
策略 pin 与实际 SPKI SHA-256 相符；有效期检查均超过一小时。新证书的
serverAuth/clientAuth 与成员/公共 peer DNS 检查通过，旧证书的公共 peer
DNS 检查通过。实际新 CA 单独验证旧叶证书失败，旧 CA 验证三个新叶证书
都失败，双 CA 验证双方成功；没有通过保留过宽信任伪造过渡结果。

三个 Secret 的 `create --dry-run=server` 均成功，随后确认候选名称在现场
仍不存在。第一阶段旧证书双 CA 的 StatefulSet 补丁带 UID/resourceVersion/
完整模板 test，server dry-run 成功；将唯一的 peer Secret 名称变化还原后，
候选 spec 与原 spec 完全一致。现场仍为 generation 38、3 Ready。

这些 dry-run 对象的 UID 不是未来实际创建身份，补丁中的 resourceVersion
也不得事后盲用。执行前仍须补齐有界阶段驱动、功能探针、实时身份与
Pod/PVC/PV 归属记录及恢复路径；不能把离线材料就绪表述成真实迁移完成。

## 滚动前实际三成员读写与卷基线

2026-09-18，`preflight.mj2Kib6B` 完成现场基线检查，执行 93610 终态 0。
通过仅绑定本机 loopback 的三个 Pod port-forward，使用原 client CA、有效
root 客户端证书和显式服务 DNS 校验访问 HTTPS gateway；未关闭 TLS 验证。
三个 Maintenance Status 返回同一实际 cluster ID、三个不同 member ID 和
一致的非零 leader，且 leader 对应其中一个成员。

分别向三个成员发送一个 VERSION=0 条件创建事务，每个键使用本次 owner
专属路径及独立随机值，再从三个成员逐一线性读取，九次均匹配。之后以
VALUE 相等为条件精确删除三个测试键，再从三成员分别确认九次读取均为空。
未使用范围删除、未创建租约、未施加故障或修改配置；清理仅删除本轮测试
数据，不可恢复。写入前保存了逐键清理输入，异常退出路径也仅作条件删除，
不会无条件覆盖或删除不匹配的值。

前后捕获 namespace/StatefulSet、Pod、PVC 与 PV JSON。实际 StatefulSet
UID、完整 spec、generation 38、3 Ready/current 保持不变；本地 KubeBrain、
PD、TiKV 九个 Pod 的 UID、容器状态与进程身份前后一致并全部 Ready。
所有已记录 PV/PVC 的 UID、spec 和 phase 前后相同；其中本地 StorageClass
仍为 12 Bound、2 Released，全部作为迁移前保护集合保存，未回收任何卷。
port-forward 子进程均已退出。证据及 SHA-256 清单保存在仓库外私有 owner
`peer-retirement-preparation.QjFVoLV6/preflight.mj2Kib6B`。

这证明原固定镜像在当次无故障状态下三个成员可写及正常转发，不证明原
公共流连续性、Watch/Lease 故障行为、30 秒恢复门限或证书迁移成功。尚未
创建候选 peer Secret 或开始 rollout；有界阶段执行与逆向恢复仍待完成。

## 第一阶段离线变更与回退计划器

`deploy/test-cluster/peer-trust-expand-plan.jq` 只规划原布局与“旧叶证书＋双 CA”
之间的转换，不负责创建 Secret、访问集群或驱动 rollout。调用方式为
`jq -er -f deploy/test-cluster/peer-trust-expand-plan.jq`，标准输入必须来自
受限证据文件，不能输出或提交包含 Secret data 的输入。

输入字段为 `baseline`、`current` StatefulSet，实时 `namespace` 及已记录的
`namespace_uid`，已审核的 `original_secret`、`expanded_secret` 和各自实时
`live_original_secret`、`live_expanded_secret`，以及 `mode`（expand/restore）。
Secret 收据必须来自实际创建/读取，不能用 server dry-run 身份代替。调用方
仍须验证收据来源、证书链/用途/有效期和双根内容；计划器只检查旧 cert/key
字节保持一致、根内容不同、三项数据布局和新 Secret 不可变，不解析 X.509。

计划器校验 namespace/StatefulSet 身份、Secret UID/resourceVersion/data，
只接受完整 spec 恰为原始或唯一派生的双 CA 第一阶段。扩展前要求原三个
副本已观测且 Ready/current；失败滚动后的恢复不要求 Ready，否则会阻断
恢复本身。从新叶证书或实验参数等后续阶段直接恢复会被拒绝，不能用这个
计划器绕过前述逆向信任顺序。输出带 StatefulSet UID、最新 resourceVersion
和完整 spec 的 JSON Patch test，仅替换 peer Secret 名称；已处于目标 spec
时输出空数组，**不代表 Pod、挂载或功能已验证成功**。

本地契约测试实际应用 JSON Patch，覆盖正向、幂等、非 Ready 回退、身份/
数据/spec 漂移、提前换叶证书、额外 CA key、可变 Secret、后续阶段回退，
以及计划生成后的 UID/resourceVersion/spec 并发变化。测试最初发现真实
manifest 中 `&&` 的 JSON 编码与 json-patch v4 标量比较不兼容；已让输出
匹配 Go HTML-safe 编码，并额外测试特殊字符、Unicode 分隔符及字面转义
文本，保留完整 spec 前置检查，不通过缩小校验范围规避问题。

修正后 `go test -race -count=3 -timeout=2m ./deploy/test-cluster` 通过
（2.629s），`go vet ./deploy/test-cluster` 和 diff 检查通过。现有 probe CI
已覆盖这个包；上述结果为本地验证，不冒充正在运行的旧源码 CI 结果。

namespace/Secret 读取与 StatefulSet 更新不是跨对象原子事务；调用方仍须
在写入前复核，限制外部变更、处理删除/重建和 rollout 失败，并验证运行
Pod 实际材料。这个计划器及离线测试本身不构成有界执行器或迁移完成证明。

## 第一阶段有界驱动与失败恢复

`deploy/test-cluster/run-peer-trust-expand.sh` 将上述计划器接入专用集群操作，
目前仅完成模拟场景验证，**尚未在集群执行**。它不创建 Secret、不删除卷，
也不进入独立新叶证书或实验功能阶段。入口必须显式给出：

```text
bash deploy/test-cluster/run-peer-trust-expand.sh --execute \
  expand|restore RECEIPT SHA256 NEW_OUTPUT_DIR KUBECONFIG CONTEXT VERIFIER
```

receipt 为计划器格式的已审核快照，内含敏感 Secret data；必须在仓库外受限
目录准备，并绑定其 SHA-256。驱动拒绝重用输出目录，在其中以 0600 保存
输入、驱动、计划器和校验脚本副本。每次尝试记录 exit-code，失败恢复另记
recovery-exit-code；恢复成功不将原失败改成成功。

读 API 和 patch 分别有 20 秒进程超时、15 秒请求超时；rollout 有 300 秒
等待及 310 秒外层超时；校验脚本有 120 秒超时。驱动在 preflight、server
dry-run 后重新读取 namespace、STS 和两个 Secret，通过计划器取得新的
并发前置条件后才更新。前后均保存 Pod/PVC/PV 身份。rollout 后要求目标
spec、三个 Ready/current 副本、Pod owner/revision、peer 卷、容器 image/
args/mounts 匹配；功能校验后再检查配置与本实例 Pod 身份和状态未变。

VERIFIER 必须是事前审核的独立 Bash 文件，接受五个位置参数：MODE、
PHASE（preflight/after）、本次 EVIDENCE、KUBECONFIG、CONTEXT。它负责真实
证书链/用途/有效期及收据审核，after 还须验证实际挂载材料和三成员正常
读写。恢复 preflight 不得要求失败实例本身已 Ready，以免阻断回退。
模拟测试中的空操作校验脚本仅为测试替身，禁止拿来做现场准入。

扩展更新结果不确定、rollout 失败、超时或运行后校验失败时，驱动不重试
扩展，而是在新 recovery 目录中读取当前身份，尝试一次受同样保护的恢复。
发现外部 spec 漂移则拒绝覆盖并报告人工复核；恢复模式不会再次递归恢复。
已处于第一阶段目标 spec 的恢复执行也必须做运行后校验，不能把空 patch
当作恢复成功。驱动不会强删未就绪 Pod 来处理 StatefulSet 强制回退问题；
若控制器未能自动回滚，在有界等待后保留失败证据，需按具体 Pod UID 另行
审核处置，不盲删 Pod 或卷。

模拟命令测试覆盖成功、写入前拒绝、已扩展实例准入失败、API 冲突、外部
spec 漂移、更新已应用但响应丢失、rollout 失败/模拟超时和功能校验失败，验证恢复后的精确原 spec、
原失败状态保留、漂移不覆盖、文件权限及同一尝试不可重跑。这些不是实际
API 故障注入、kubelet/证书迁移或 30 秒服务恢复验收。现场仍需准备并审核
具体 VERIFIER、Secret 实际创建收据、卷保护集合及实验后的精确清理流程。

最终版本本地 `go test -race -count=1 -timeout=2m ./deploy/test-cluster`
通过（52.499s，执行 24943 终态 0），`go vet ./deploy/test-cluster`、Bash
语法与 diff 检查通过。未执行现场驱动，实例仍为原 generation 38。远端
`dcca4197` 的 probe run 35334686727 已报告 success，image run 35334686689
仍在 Verify published test image 阶段；不将这些旧源码 CI 状态视为本驱动
的 CI 验证，也不在其运行期间推送取消构建。

## 第一阶段证书材料校验器及真实材料复核

`deploy/test-cluster/verify-peer-trust-material.sh ORIGINAL_DIR EXPANDED_DIR
NEW_MEMBER_DIR PEER_DNS` 是离线校验器，各目录含 ca.crt、tls.crt、tls.key，
只接受可信私有目录中的普通非符号链接文件（各不超过 1 MiB）。它不是
运行时凭据加载器，不能用于攻击者可并发替换文件的目录，也不是驱动所需
完整 VERIFIER 的替代品。

校验限定本次单旧 CA、单新 CA、双 CA 过渡布局，要求原 cert/key 字节不变，
双根除空行外精确等于旧根后接新根，不允许额外证书、注释或私钥 PEM 块。
旧、新 CA 指纹及成员 SPKI 必须不同，两个叶证书分别与 key 匹配；根和叶
剩余有效期超过一小时，并通过显式 CA（不使用系统信任目录/存储）的当前
时间、DNS、sslserver/sslclient 验证。双 CA 必须接受双方，而两套单 CA
必须分别拒绝对方叶证书。输出只含离线通过标记，不输出 key。

本地测试包含有效材料、空行、错误 DNS、过短有效期、缺 clientAuth、错 key、
跨 CA 复用成员 key、提前替换旧叶证书、缺旧根、额外根、CA 数据夹带私钥及
重复同一身份。初始测试夹具因 CA 与叶证书同名导致 OpenSSL 路径校验失败，
已修正夹具；不能将其记成产品证书轮换失败。

实际 `material-check.KVdBjJp9` 初次复核退出 1，原因是原准备脚本为 CA 拼接
加入空行，而初版校验器要求字节完全连续；失败目录原样保留。修正为仅忽略
空行后，在新 `material-recheck.N9McdFy1` 中对三个真实成员全部通过，执行
终态 0。该目录保存校验器副本、三个无密钥输出的日志和 SHA-256 清单，原
Secret 解码文件仍仅在前一受限 owner 中。没有创建 Secret、修改 StatefulSet
或轮换实际服务证书；现场挂载/握手及阶段读写校验仍须接入完整 VERIFIER。

最终本地 `go test -race -count=1 -timeout=2m ./deploy/test-cluster` 通过
（54.948s，执行 99124 终态 0），vet、Bash 语法及 diff 检查通过。
远端 dcca4197 的 image/probe/backend 三个 run 均已终态 success；API 元数据
已保存在 `ci-dcca4197.P8woEvJj`。这是 CI 状态证据，尚不替代该版镜像摘要、
构建信息与节点实际二进制的独立校验，也不覆盖后续本地提交。

## 当前三实例实际挂载与 mTLS 信任基线

2026-09-18，`runtime-baseline.Mxky8PCp` 对实际三个 kubebrain-local Pod 完成
只读运行时检查，执行 16057 终态 0。每个 Pod 内执行 sha256sum，ca.crt、
tls.crt、tls.key 三个挂载文件均匹配已捕获的原材料；不读取或输出 key 内容。
随后通过仅绑定 loopback 的 peer 3380 port-forward，使用显式旧 CA、公共
peer DNS 和原叶证书 SPKI pin 发起 HTTPS 请求，不使用 insecure TLS。

每个实例依次执行三次新的 TLS 连接：旧客户端身份访问不存在的只读路径
得到 HTTP 404；新 CA 签发的该成员身份被拒绝，curl 退出 56、HTTP 状态 000，
明确收到 TLS `unknown ca` alert；再次使用旧身份仍得到 HTTP 404。最后
一次正向连接排除了把停止监听或一般网络中断误判为证书拒绝。404 在这里
只证明已通过 mTLS 并到达 HTTP 路由，不证明业务读写或控制协议可用；正常
写读证据仍来自上文独立 preflight。

前后确认 namespace/StatefulSet UID、原完整 spec、generation 38、3 Ready/
current 不变；三个 Pod UID、spec 和容器进程状态一致。所有 port-forward
进程均已退出，对应本机端口无监听。证据包含原/后资源 JSON、三个挂载
哈希、正负请求状态及错误、SHA-256 清单，均位于仓库外私有 owner。

这证明当前服务的实际挂载与旧单 CA 信任边界，而非新双 CA 已启用。未创建
候选 Secret、未重启 Pod、未操作数据卷；仍须将相同检查和三成员写读一起
接入阶段 VERIFIER，扩展后断言新身份也被接受，恢复后再次断言其被拒绝。

## 阶段 VERIFIER 已接好并验证原配置

`deploy/test-cluster/verify-peer-trust-stage.sh` 实现驱动约定的五参数接口。
在受 SHA-256 绑定的 receipt 中增加 verification 字段：bundle_dir、
client_tls_dir、material_verifier、material_verifier_sha256、peer_dns、
client_dns、cluster_id（全部为字符串）。路径须指向已审核的私有材料；
material_verifier 是上文材料校验器，复制后再次检查摘要。不得使用包含
测试替身、任意第三方脚本或未经审核材料的 receipt。

preflight 从原/候选 Secret data 解码本轮私有文件，验证三个新成员材料，
以及用于应用读写的客户端证书有效期、客户端用途、CA 链和 key 匹配。
此阶段无集群访问，不把服务已 Ready 作为恢复前置条件。after 检查实际
Pod UID/owner/spec 与驱动捕获的对象匹配、三个挂载文件哈希正确，以旧根、
明确 DNS 和原叶证书 SPKI pin 进行旧/新/旧三次独立连接：expand 要求三次
均到达 HTTP 404；restore 要求新身份收到明确 TLS 证书拒绝、其余成功。

接着通过三个实际 Pod 的客户端端口分别取得一致 cluster/leader、不同
member ID，创建三个随机 owner 专属的 VERSION=0 条件写入键，交叉线性
读取九次；用 VALUE 条件精确删除后，再交叉确认九次不存在。所有不确定
写入前保存清理输入，异常退出尝试条件清理，失败则保留证据并返回失败。
结束前核对 Pod 进程状态未变，清理 port-forward。只删除自己的临时测试
键，不做范围删除或租约/卷操作。脚本耗时受外层驱动 120 秒校验门限约束。

现场 `stage-hook-baseline.lq6jbAb6` 的 restore/preflight 和 restore/after
均成功，执行 4597 终态 0；这里的 restore **只是校验当前原状态的模式**，
没有执行恢复更新。记录包括挂载、真实 mTLS 正负请求、三写九读及精确清理。
随后最终脚本增加客户端材料准入检查，该 preflight 在真实材料上通过；
还故意用 expand/after 校验当前单 CA 实例，正确因首个 Pod 挂载根不匹配
退出 1，在启动 port-forward 和任何测试写入之前拒绝。该负向验证执行
46693 终态 0（断言预期失败成立），不是扩展通过。

验证用 receipt 明确为 verification_only，候选 Secret 尚未真实创建，不能
直接送给迁移驱动。最终实际 STS UID、完整 spec、generation 38、3 Ready
不变；六个本机转发端口均无监听，测试数据已精确删除且不可恢复。旧/新
材料均未改写，未触碰数据卷。现场仍缺实际 Secret 创建收据、第一阶段
expand/after 成功与受控恢复证据；不能将这个原配置检查当作迁移完成。

本地完整 `go test -race -count=1 -timeout=2m ./deploy/test-cluster` 通过
（60.617s，执行 7926 终态 0），vet、Bash 语法和 diff 检查通过。新增自动
测试覆盖两个模式的离线准入，以及校验器摘要改变、根/私钥不匹配、应用
客户端错误 CA/key、不安全 peer 配置与非法模式的拒绝，并断言 preflight
不会调用 Kubernetes。正在运行的 02786d91 CI 不包含本次新增阶段脚本。

## 第一阶段现场扩展、恢复及精确清理（2026-09-18）

私有 owner `peer-retirement-preparation.QjFVoLV6/live-first-phase.8u66oAH6`
保存了新的现场快照和固定脚本副本。准入重新核对 namespace/StatefulSet
UID、原完整 spec、generation 38、三个 Ready 副本、原 Secret UID/RV/data，
以及三个新成员证书有效期。保护集合捕获所有原 PV，其中本地 StorageClass
为 12 Bound、2 Released。当前原镜像保持先前已审核的固定摘要，未部署
仍在 CI 的 02786d91 镜像，也未把本地脚本测试表述成该版 CI 验收。

随后实际创建不可变 Secret `kubebrain-local-peer-old-dual-qx4iltrm`，UID
`b91298a7-ecd2-4293-b586-bd5b33b22527`。原 cert/key 字节不变，只有 CA 信任
扩展为已审核的旧＋新两根；没有新叶证书挂载、子目录切换、实验参数或
镜像变化。驱动经 server dry-run、重新读取对象和完整 spec/UID/RV 前置
检查更新 StatefulSet，三个成员依序滚动至 generation 39。

expand 驱动及 after 校验均成功：每成员三个文件挂载哈希正确，旧、新、
旧三个独立 TLS 客户端连接均到达 HTTP 404；服务端仍是原叶证书，DNS、
显式旧 CA 与 SPKI pin 验证开启。新证书这里只用于测试客户端身份，不
表示新服务端证书已部署。三成员分别完成条件写入、九次交叉读取、精确
条件删除及九次不存在确认。原 30s/25s/500ms 选举参数及默认 2PC 不变。

同一执行器随后显式恢复原单 CA Secret，三个成员依序滚动至 generation
40。restore 驱动和 after 校验均成功：原完整 spec/固定镜像恢复、三个
Ready/current 副本；三个新身份分别收到明确 unknown-ca TLS 拒绝，旧身份
前后均连接成功。恢复后的三写九读与精确清理也通过。主执行 13279 终态
0，两阶段 exit-code 均为 0；不是由失败恢复分支掩盖的成功。

清理前按扩展阶段 Pod UID → ephemeral PVC owner UID → PV claimRef UID
确定六个中间临时卷，逐个确认不在原保护集合、原 Pod/PVC 已不在场、没有
当前 PVC 使用、PV 为 Released/Retain 且完整 spec 和 CSI driver 匹配。
只对这些 PV 用 UID/RV/spec/phase JSON Patch 前置条件改为 Delete，并
等待实际删除。检查当前 Pod/controller 无引用后，以 UID/RV 前置条件删除
本次 Secret。清理执行 71341 终态 0，六个临时卷数据不可恢复；Secret 材料
仍有私有离线备份，不是删除原共享 Secret。

所有原 PV 的 UID 和 spec 均保留。最终本地存储为 12 Bound、8 Released：
额外六个 Released 是滚动前已经存在的临时卷，本轮按原保护集合保留，
不是遗失归属后泛化清理。六个数据卷和旧 Ceph 卷未删除或改策略，当前
六个临时卷属于恢复后的 Pod。临时编译的 uid-delete 二进制已精确删除，
源码和摘要保留；所有本机 port-forward 已退出。

核心证据：baseline、created-secret.json（敏感）、live-receipt.json（敏感）、
expand/restore 的各阶段资源和运行时校验、cleanup 的目标与前置条件/删除
证据、verified.json。源脚本、两阶段校验及清理 SHA-256 清单均复核通过。
这次证明第一阶段真实信任扩展及恢复，不证明滚动过程零中断、公共长流
连续性、独立服务端成员证书迁移、交接协议、原 30 秒故障门限或生产就绪。

## 第二阶段相邻转换计划器

同一离线计划器新增显式 `phase: "members"`，默认仍为 `roots`，后者输出
与第一阶段兼容。members 输入额外提供实际 member_secret 收据及实时
live_member_secret；baseline 仍为原完整 spec，expanded_secret 仍为共享
旧叶证书＋双 CA。这样三个状态都从同一可信原始 spec 确定性派生，不允许
以任意当前配置充当“可回退基线”。

members/expand 只接受已完成双 CA 的共享旧叶证书配置，或同一派生的独立
成员配置；变更前要求三个副本均 Ready/current。members/restore 只恢复到
共享旧叶证书＋双 CA，失败滚动时不要求 Ready。roots/restore 明确拒绝从
独立成员配置直接跳到原单 CA。所有更新仍带 UID/RV/完整 spec test，成员
切换仅改变经现有挂载片段限定的 peer 卷和 subPathExpr，不修改镜像、参数、
数据卷、资源或其他 TLS 配置，也不启用实验协议。

成员 Secret 必须不可变、与两个旧 Secret 身份不同且 UID/RV/data 与实时
对象匹配；必须恰好包含三个成员各四个条目。每份 CA 字节必须保持同一
双根集合，cert/key 不得沿用旧共享值或在成员间重复。规划器不解析 PEM
或策略 JSON，编码不同但语义相同的 key 仍需真正的 SPKI/证书及策略校验
拒绝，不能把字符串检查当作密码学身份保证。挂载前置条件包括唯一 peer
挂载、只读原路径、POD_NAME 来自 metadata.name、无 init/临时容器、无
旧子路径和无 rolling partition，避免派生出扩大 key 暴露面的布局。

测试将生成补丁实际应用于完整 StatefulSet，再与既有 StrategicMergePatch
片段的结果逐字段比较；覆盖正向、幂等、未就绪回退、禁止跳阶段、重复
密钥、根变化、额外 CA key、缺策略、可变 Secret、身份和配置漂移，以及
生成计划后的 UID/RV/spec 并发变化。成员定向测试三轮 race 通过（4.552s）。

当前现场执行器和阶段 VERIFIER **仍显式只接受 roots**，members 输入在
执行器创建尝试目录或访问 API 前即被拒绝；VERIFIER 也拒绝该阶段。必须
先扩展实际 Secret 重新读取、成员材料／服务端 pin／隔离挂载验证及对应
回退路径，才能解除这一限制。第二阶段尚未在集群执行，当前仍是恢复后
generation 40 的原配置。

完整本地包测试 `go test -race -count=1 -timeout=2m ./deploy/test-cluster`
通过（62.304s，执行 31661 终态 0），vet、两个 Bash 入口的语法和 diff 检查
通过。02786d91 的 probe run 35337650907 已终态 success，image run
35337650967 仍在构建；这些 CI 不覆盖本次新增成员计划器。
