# DBaaS 验收状态

核验日期：2026-09-09。当前验收环境为 `tk-001-003`；旧 `kind-kubebrain-dbaas` 结果单独保留在本页历史部分，不作为新环境现状。
产品要求及兼容性矩阵见 [兼容性计划](dbaas_compatibility_plan_cn.md)。本页列出当前证据的边界与下一步验收条件，不能代替完整矩阵。

总体状态：**尚未通过生产就绪验收**。已完成迭代编号、提交数和单元测试数量都不是整体完成百分比。

用户授权使用 `root@10.32.32.66` 控制的 `tk-001-003` 集群，并明确只使用 rook-ceph 消费者存储、禁止使用 rook-ceph-secondary。连接路径、安全边界、资源 UID、日志和执行结果统一记录于 [tk-001-003 测试环境交接记录](test_environment_tk_001_003_cn.md)。凭据与验收工具保存在仓库外私有目录，不写入本页。

## 当前环境与发布验收

本表区分正在服务的版本、本地候选验证和真实发布结果；测试通过不能跨版本、拓扑或故障范围外推。

| 验收项 | 已取得的证据 | 仍未证明的范围/下一步 |
| --- | --- | --- |
| 分支与源码对标 | `dbaas` HEAD 为 `4f9a3eb19e2dc42278d37b6af938246d68e269f1`；产品提交 `05032758`；本地只读 etcd 基线仍为 `5cd9f4ee13801e18825d661e5005ae599460bc3a`，工作树干净；当前 go.mod 的 etcd API/client/server 为 v3.7.1 | 这些是本地固定版本，不表示已跟踪远端最新版本或完成支持版本矩阵 |
| 实际服务版本 | KubeBrain 三副本已部署，当前仍使用 `339381af` 的不可变镜像 `sha256:a245c95fea36c387358d86e3808a9d29073a327028d5a4e3a80e4d272663e865`；StatefulSet generation/observed 为 3/3、current/update revision 均为 `kubebrain-696c87f8f9` | 尚未部署本次 05032758 产品候选，不能将本地修复视为线上已生效 |
| 副本与放置 | 新鲜只读查询：KubeBrain/PD/TiKV 各 3/3 Ready、各容器 restart 0，同组件三副本分布在 `k8s3-worker1/2/3` | 节点无 topology zone/region 标签；物理宿主机及跨可用区独立性未证明，节点/网络故障须先确认授权范围 |
| 数据卷与后端健康 | 六个 PD/TiKV 数据 PVC 均 Bound，使用 `nvme-rep3-rbd-pool`；driver 为 `rook-ceph.rbd.csi.ceph.com`、clusterID 为 `rook-ceph`；本轮原样后端健康门禁 exit 0，含六卷实际 CSI 身份/容量隔离/Retain 与连续三次无异常 Region 检查 | StorageClass 默认 reclaimPolicy 仍为 Delete，不能与六个现存数据 PV 的 Retain 混为一谈；新增数据卷仍需逐卷验证保留策略；瞬时健康不等于故障恢复和长期稳定性 |
| 已有受控可用性验收 | 当前服务版本此前通过受控 leader Pod 删除：900/900 操作、public watch 900、三个 direct watch 各 900、lease 存活、官方 etcdutl restore 校验 | 不覆盖无主动释放的崩溃、节点失联、网络分区，也不是新候选升级验收 |
| 冷镜像在线升级 | de8a9e1f 的真实冷升级曾因 direct watch 超过 30s 门限失败并回滚；镜像拉取约 36.973s | 失败仍未关闭。05032758 增加隔离预拉取、运行时摘要核验与失败清理，但尚需新候选真实准备及完整升级验证，不能仅以缓存已热的重跑证明修复 |
| 当前候选本地门禁 | 05032758 提交前/后各 720 项四分片及 inventory 均通过，组件/CLI/build race 通过；已预编译恢复工具并验证缺少镜像回执时入口停止 | 本地测试不替代实际控制器、CRI 拉取、真实业务探针和回滚清理证据 |
| 当前镜像发布 | 用户恢复 Runner 后，原 CI [34344914914](https://github.com/fivetime/kubebrain/actions/runs/34344914914) 已执行，但在预拉取组件测试阶段 failure：两个成功场景的准备过程超过共用测试配置的 1s 上限；安全扫描及架构检查已通过，镜像构建步骤未执行 | 本地已复现并调整测试预算，保留显式短超时/补偿清理断言，10 轮 race 通过；完整提交门禁执行中。之后需要新提交的新 CI、精确镜像核验和升级验收，不能继续使用绑定旧失败 CI 的部署入口 |
| 总体验收 | 原环境的兼容性、恢复和压力实验仍作为各自范围的历史证据 | 新环境真实 apiserver 路径、新版本在线升级、后端 TLS/轮换、数天级 watch/故障恢复 soak、生产规模/版本矩阵，以及管理面、计量和外部系统验收仍开放；以完整兼容性计划逐项验收 |

本轮只读证据位于私有交接目录的 `security-prepull-acceptance-{serving-state,pods,storageclass,nodes}.json`；
后端健康日志为 `security-prepull-acceptance-backend-health.log`。该日志的 `max_disk_used_percent=90` 是拒绝阈值，
不是实际磁盘使用率。当前交接证据不依赖旧 `/tmp` 镜像归档，但仍需按正式发布要求持久归档。

下一步：等待原 CI 获得执行器并成功，独立核验发布镜像，重新验证精确集群/存储/健康身份，执行隔离预拉取和
6000 次探针的真实在线升级；其后继续完整计划中的恢复、TLS、真实消费者和长时间验收。未获得授权前不操作
worker、PD/TiKV 重启或网络故障，不使用 secondary Ceph。进行中的状态文档暂不 push，避免取消原 CI。

## 历史基线：A5788 / kind（2026-09-08）

以下表格和收尾记录仅描述当时的 kind 环境，其单节点、hostPath、镜像和部署待办不能套用到当前 tk-001-003。

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

下一步顺序：完成[安全升级门禁](security_baseline_20260908_cn.md)，取得新基线 CI 镜像后部署 KubeBrain 并完成客户端语义测试，再推进 P2 恢复、故障历史与长时间测试。原 kind 环境及其 hostPath 卷保留，不迁移或覆盖；新环境固定使用仓库外独立 kubeconfig。三 worker 的物理宿主机/可用区独立性尚未证明，节点级故障还涉及用户已有工作负载，需先确认具体授权范围。

原 kind 部署入口复核：`deploy/production/kubebrain.yaml` 和 `deploy/production/tidb-cluster.yaml` 均要求同组件副本按 `kubernetes.io/hostname` 硬反亲和，不能在该单节点上满足三副本调度；zone 规则仅为偏好，模板本身不证明跨区放置。该 kind 集群 `CSIDriver` 数量为 0，CRD 清单没有 VolumeSnapshot、Prometheus 或 ServiceMonitor API。现有模板仍需目标环境注入镜像、存储、证书、网络与监控配置，不能直接 apply 到实验实例。上述是相应部署/恢复/计量验收的环境前置缺口，不应记作已经复现的 etcd API 语义缺陷，也不代表所有产品代码工作都依赖这些环境。

2026-09-08 构建清理：按明确 ID 回收 84 条 KubeBrain 编译缓存（约 348 GB），删除 1,744 个旧 OCI/解包临时文件（41,548,937,075 bytes）及 3 个本地编译二进制；未删除源码、测试记录或数据卷。根分区可用空间由约 1.7 GiB 恢复至 327 GiB；本轮镜像导入后约 324 GiB。旧实施记录中的 `/tmp` 镜像归档位置是历史位置，清理后不再代表可恢复实物；对应构建产物需要从记录的提交重新生成。

当前 OCI 和日志保存在 `/tmp` 的 tmpfs 上，只能视为本次运行的临时证据。重启或清理会丢失，生产验收还需要持久保存镜像、日志和验收结果。

A5788 回滚终态：StatefulSet generation/observed 为 `1012/1012`、resourceVersion `9119057`，22 个启动参数不变；runtime 恢复稳定摘要 `sha256:bc6b443ff3508482908155bfaf234d924305f1dce215fd8f7de14093e83d899b`。候选与稳定门禁的 revision/HashKV 均为 `75044/1984703050`，term 随滚动选主由 `894` 变为 `896`。候选 CRI container/image、containerd 镜像引用及四份关键 manifest/config content 已清零；六个临时端口监听均已关闭，PD/TiKV 六副本仍 Ready，三个 store 为 Up。未触碰其他轮次运行时索引。
