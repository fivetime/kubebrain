# DBaaS 验收状态

核验日期：2026-09-08。范围为 `dbaas` 分支和 `kind-kubebrain-dbaas` 实验集群。
产品要求及兼容性矩阵见 [兼容性计划](dbaas_compatibility_plan_cn.md)。本页列出当前证据的边界与下一步验收条件，不能代替完整矩阵。

总体状态：**尚未通过生产就绪验收**。已完成迭代编号、提交数和单元测试数量都不是整体完成百分比。

| 验收项 | 当前证据 | 尚需取得的证据 |
| --- | --- | --- |
| 分支与源码对标 | 当前为 `dbaas`；本地 etcd 基线为 `5cd9f4ee13801e18825d661e5005ae599460bc3a`，etcd 工作树干净 | 支持版本窗口内的完整差分结果；本地 HEAD 不代表已查询远端最新版本 |
| A5788 镜像引用门禁 | 代码 `aca51b7f`；三个 RED 已复现误放行；修复后 focused/race、完整 readonly probe、提交前后四分片通过；候选 generation 1011 完整门禁及回滚后 generation 1012 稳定适用门禁通过，均 3/3 Ready、restart 0 | 本轮子项已完成；不证明整个产品已生产就绪 |
| 数据面副本健康 | 3 KubeBrain、3 PD、3 TiKV 当前 Ready | Ready 只证明当前服务状态，需另外验证隔离、恢复、故障历史和 SLO |
| 故障域隔离 | 集群只有 `kubebrain-dbaas-control-plane` 一个节点，无 zone 标签；6 个 PD/TiKV Pod 均运行在该节点 | 真实独立节点/故障域部署及对应分区、节点故障、恢复测试；同宿主多容器不足以证明跨可用区能力 |
| 持久卷与容量隔离 | 唯一 StorageClass 为 `standard`，provisioner 为 `rancher.io/local-path`；PD/TiKV 数据 PV 为 hostPath，共享宿主文件系统 | 独立 CSI 卷的 driver/volumeHandle/claimRef、实际容量及剩余空间通过严格存储门禁 |
| 存储生产门禁 | 清理后原样执行 `KUBE_CONTEXT=kind-kubebrain-dbaas hack/production/validate-tikv-region-health.sh` 仍返回 1；磁盘压力已消除，6 个卷各有 CSI 身份和容量隔离错误 | 修复实际基础设施后原样通过；不能放宽阈值或改写 hostPath 为 CSI 元数据 |
| 恢复与长期验证 | 计划 P2 明确保留真实 CSI target retirement→provisioning→durable restore、加密 key promotion/撤权、版本矩阵、生产规模和长时间 soak | 对每个开放项提供新鲜、可追溯的真实执行结果；已有实验结果不能外推未测拓扑/规模 |
| 管理面与计量 | 计划 P1 仍列出跨 cluster/region 调度、管理面/外部 IdP HA soak、预生产 Prometheus evaluation/连续 24 小时采样及外部财务系统真实回执等开放项 | 本轮未验证这些外部系统；须按完整计划分别取得实际证据，不能由数据面门禁替代 |

下一步顺序：把验证工作推进到独立存储和真实故障域环境，再执行 P2 开放的恢复、故障历史与长时间测试。当前 kubeconfig 仅有 `kind-kubebrain-dbaas`，唯一 StorageClass `standard` 的 provisioner 为 `rancher.io/local-path`。下一环境需要明确已授权的 context/kubeconfig 路径、namespace、节点/故障域、CSI StorageClass 及持久证据归档位置；不得自行购置云资源或迁移现有数据卷来制造验收结果。

部署入口复核：`deploy/production/kubebrain.yaml` 和 `deploy/production/tidb-cluster.yaml` 均要求同组件副本按 `kubernetes.io/hostname` 硬反亲和，不能在当前单节点上满足三副本调度；zone 规则仅为偏好，模板本身不证明跨区放置。当前 `CSIDriver` 数量为 0，CRD 清单没有 VolumeSnapshot、Prometheus 或 ServiceMonitor API。现有模板仍需目标环境注入镜像、存储、证书、网络与监控配置，不能直接 apply 到实验实例。上述是相应部署/恢复/计量验收的环境前置缺口，不应记作已经复现的 etcd API 语义缺陷，也不代表所有产品代码工作都依赖这些环境。

2026-09-08 构建清理：按明确 ID 回收 84 条 KubeBrain 编译缓存（约 348 GB），删除 1,744 个旧 OCI/解包临时文件（41,548,937,075 bytes）及 3 个本地编译二进制；未删除源码、测试记录或数据卷。根分区可用空间由约 1.7 GiB 恢复至 327 GiB；本轮镜像导入后约 324 GiB。旧实施记录中的 `/tmp` 镜像归档位置是历史位置，清理后不再代表可恢复实物；对应构建产物需要从记录的提交重新生成。

当前 OCI 和日志保存在 `/tmp` 的 tmpfs 上，只能视为本次运行的临时证据。重启或清理会丢失，生产验收还需要持久保存镜像、日志和验收结果。

A5788 回滚终态：StatefulSet generation/observed 为 `1012/1012`、resourceVersion `9119057`，22 个启动参数不变；runtime 恢复稳定摘要 `sha256:bc6b443ff3508482908155bfaf234d924305f1dce215fd8f7de14093e83d899b`。候选与稳定门禁的 revision/HashKV 均为 `75044/1984703050`，term 随滚动选主由 `894` 变为 `896`。候选 CRI container/image、containerd 镜像引用及四份关键 manifest/config content 已清零；六个临时端口监听均已关闭，PD/TiKV 六副本仍 Ready，三个 store 为 Up。未触碰其他轮次运行时索引。
