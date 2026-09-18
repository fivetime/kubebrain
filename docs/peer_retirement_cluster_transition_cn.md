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
