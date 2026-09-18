# 专用集群 peer 身份迁移与恢复准备

状态：离线挂载片段和契约测试已准备，**尚未执行迁移**。本页不表示新镜像
或原 30 秒故障验收已经通过。目标仅为 kubebrain-dbaas-test/kubebrain-local，
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
