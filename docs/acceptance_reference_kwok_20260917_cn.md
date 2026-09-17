# 参考 etcd 的真实控制器与 KWOK 联动验证

2026-09-17，本机隔离参考运行 session 91657 成功退出 0；
`operation_exit=0, backend_cleanup_exit=0, runner_exit=0`。这是参考 etcd 结果，
**不是 KubeBrain/TiKV 后端验收，也不是实际容器运行、HA 或规模测试。**

## 固定来源及隔离

本地源码基于 `97055dd8` 加本次工作树修改；测试目录保存脚本/配置摘要。
apiserver、controller-manager、scheduler 为已核验官方 v1.36.1；参考 etcd 为
`5cd9f4ee13801e18825d661e5005ae599460bc3a`。KWOK 为干净源码
`099ce5faf29193ac19f0d7529103327c48570f20` 的 Go 1.26.8 构建，二进制摘要
`4ec439887e6e1e6f333235d481fb6b22d3d1c652aa53cc0b73e70b32ce2e1a6c`。
五个二进制均在启动前核验摘要；未安装系统服务，未接触管理集群或共享存储。

独立 CA、各组件独立 kubeconfig、loopback API 18453、参考 etcd 13579/13580。
KWOK 使用独立空工作目录及明确的静态 Stage，不加载宿主默认配置；HTTP 服务
地址为空，不启用 CRD。仅管理 `reference-node`，节点 Lease 时长为 40 秒。
etcd、apiserver、controller-manager、scheduler、KWOK 均由有界进程所有者管理，
结束后逆序停止并回收，三个监听端口均释放。认证材料和临时数据保留私有，不入库。

## 实际权限与写入链

KWOK 实际证书用户名为 `kubebrain-test-kwok`，不含 system:masters；真实
SelfSubjectAccessReview 检查允许指定 Node status、测试 namespace Pod status、
指定 Node Lease 更新；拒绝其他节点、其他 namespace 的状态更新、创建
Deployment/Pod、Pod binding、Secret 读取、ClusterRole 创建和 Lease 创建。
这只是所列权限检查，不宣称枚举了集群所有 API 权限。

驱动预创建空 Node Lease，KWOK 自行接管并续租，没有为方便扩大到 Lease create。
启用真实 node-lifecycle-controller 和 taint-eviction-controller；KWOK 写入 Ready，
节点生命周期控制器处理初始污点，驱动没有手动 patch Ready 或移除污点。

真实 Deployment 控制器创建一个 ReplicaSet，真实 ReplicaSet 控制器创建三个
Pod，真实 scheduler 将其绑定到 reference-node；原 UID 所有权链和审计检查通过。
KWOK 再写入三个 Pod 的模拟 Running/Ready。Deployment 实测 readyReplicas=3、
availableReplicas=3、observedGeneration 匹配；不等于真实 kubelet 启动了容器。

Node Lease UID `e1a7c804-0fb2-4a59-ab33-f584c268427a`，同一 holder 的 renewTime
从 `08:29:09.181591Z` 到 `08:29:19.283428Z` 增加。新增 RequestResponse 审计只
覆盖 KWOK Node/Pod status 和 Lease patch/update，不捕获 token 响应。审计核对
实际 KWOK 用户、成功响应、对象 UID、Ready/Running 状态以及上述两次精确
renewTime，结果为 true。不是仅看最终状态或由管理员代写状态的测试。

日志仍记录启动期空 manifest、kube-system 尚未创建时的 Lease 重试、endpoints
清理，以及 scheduler 停止时 event broadcaster 已停止；保留原始日志，不称零错误。

## 回归与证据

九个审计用例覆盖通过及缺 Node/Pod、错误用户/UID、失败更新、缺续租、不前进的
renewTime、缺节点 UID。准入新增错误模式、缺 KWOK 二进制、错误摘要拒绝，
均须发生在执行任何二进制或创建工作目录前。组合 race 通过（10.380 秒），
KWOK/准入单独 race 通过（1.426 秒），vet、shell 语法和 diff 检查通过。
最初 shell 语法命令在错误工作目录运行而报找不到文件，随后从仓库根目录重跑通过；
不将该命令路径错误作为功能回归失败。

另以 `CONTROLPLANE_KWOK=false` 重跑默认真实控制器调度路径，session 25842
退出 0，`controlplane-reference.e9mIxri1`。该默认模式仍不声明 Pod Running。
共享后端仍保留此前候选试验两条空长租约，本轮未读取、续租、撤销或变更它们。

主证据：`/root/.local/state/kubebrain/kwok-controlplane-reference.3YaXBQbV/`，
子目录 `controlplane-reference.Si29e6U2`，含 RBAC 响应、对象快照、审计、
脚本摘要及 result.json。后续需同源 CI、新鲜共享后端准入和实际 KubeBrain
KWOK 对照，随后才可扩展规模、更多控制器与故障场景，不能据此宣称生产就绪。

后续同源回归 CI `35200243908` 已成功，源码
`3e813b998ff78d8da6f34894512f9d9619a5271e`，attempt 1。日志确认 etcd 全包
137.247 秒；Auth、Lease、Watch/ReadBarrier race 分组分别 85.805、76.703、
25.366 秒；follower proxy 5.278 秒、revision barrier 8.360 秒，探针全量 race
332.461 秒。此 CI 不运行上述本机完整控制面/KWOK 场景，二者证据不能混用。
日志已归档至私有 `release-3e813b99.eExmyBVn/probe-ci.log`，SHA-256 为
`da398d2995e41d3fa37b20390fb703e0e1475630699740d1283811d6ba9285d9`。
记录时镜像 CI `35200203854` 仍在编译推送，尚无独立发布核验结论或新候选部署。

09:02 UTC 后续：镜像 CI `35200203854` 亦已成功，同源独立发布核验 session
1372 退出 0，证据 `release-3e813b99.eExmyBVn/audit.ZBtxjwnh`。发布 index 为
`sha256:172a8b6caceac6a4094354e7589f1bcf972acc9ba3e2e3cfae3099690e62ddde`，amd64 为
`sha256:45d8b6323f07e2046841f295d293ffd7571b83ea919fa03f514698d8cb94e6c5`，arm64 为
`sha256:0502b76003657b9a6b8b1cafa7c2190b9e3483392720f5b385feb14791aa8931`。
已核对清单、推广标签、实际 amd64 二进制身份/构建信息及非 root 用户，临时
审计容器和提取二进制已清理。该回执只证明发布身份；候选仍为 HOLD，等待旧
租约实际归零及新鲜准入，尚未执行 KubeBrain + KWOK 对照。
