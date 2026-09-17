# 真实控制器 Pod 替换：参考 etcd 对照

2026-09-17：基于 `c3e8afdd` 新增可选 Pod 替换阶段，本机固定参考 etcd
端到端通过。**尚未在 KubeBrain 上运行该新增阶段；不是 HA、真实容器恢复、
规模或生产就绪结论。** 产品实现及产品镜像未改动。

## 新增覆盖与约束

`CONTROLPLANE_POD_REPLACEMENT=true` 必须与 KWOK 同时开启，默认关闭。
首先完成原有创建、调度、状态及写入者审计，再从这次隔离 API 的三个 Pod
快照中选一个对象，以 UID 和 resourceVersion 双重前置条件 DELETE。
没有 kubelet，因此明确使用零宽限期；只操作新建的 `controlplane-smoke`
夹具，不读取管理集群 kubeconfig，不删除管理集群或已有业务 Pod。

要求 60 秒内原 Deployment 和唯一 ReplicaSet 身份保持不变、期望副本仍为 3，
两个原 Pod 存活、被删除 UID 消失、恰有一个新 UID，三个 Pod 都由同一 RS
管理并调度到 reference-node、Running/Ready。Deployment 已观察到当前
generation，ready/available 为 3。审计必须证明全部四个历史 Pod 由 RS
controller 创建、由 scheduler 绑定；新 Pod Ready 由 KWOK 写入；唯一删除
由测试管理员发起且 UID/RV/零宽限期与选定快照一致。

KWOK 模式审计新增的响应体范围仅为测试管理员对夹具命名空间 Pod 的 delete，
不新增 RBAC 权限，不记录 Secret/token 响应体。其余后端身份、准入、恢复
及固定 60 秒租约清理门限不变。

## 本地及真实参考结果

- 18 个替换正反例：拒绝旧对象残留、替换两个存活对象、错误 owner、非 Ready、
  未调度、RS/Deployment 更换、generation 未观察、管理员代创建/绑定、缺失
  删除、错误删除 UID/RV、缺失/错误状态写入者、创建失败、重复创建。
- 审计策略精确范围及新增非法参数/无 KWOK 准入拒绝测试通过。
- 完整 `^Test(ControlPlane|Ephemeral.*PKI)` race 测试退出 0，10.682s
  （session 40186）；兼容测试包 `go vet` 退出 0（session 27035）。
  使用私有 compat.mod 将五个 etcd 模块绑定 `/root/etcd` 固定参考源码。
  首次从主模块目录运行该 nested-module 命令被包路径拒绝，未执行测试；
  修正到 `hack/etcd-client-compat` 后执行，不将前次失败计为通过。
- 真实参考 session 73233 退出 0，子结果 operation/backend_cleanup/runner
  均为 0，初始及替换状态/写入者审计均通过。reference 模式无需共享后端
  租约清理，不能以此证明 KubeBrain 的长租约清理通过。

10:57:39.632251 UTC，DELETE 200，旧 UID
`86941170-e8d1-4949-8f49-cd1de5690a47`，前置 RV `253`；新 UID
`518e898c-0169-4b1a-827a-6a38d1ca1c16`。另外两个 UID
`0d4a00b3-b7a7-44e9-9e6f-321af9e03f33`、
`b33c7b51-f619-4b36-85ce-aaf4d08dcc05` 保持不变。

使用固定官方 Kubernetes v1.36.1 三组件、参考 etcd 源码
`5cd9f4ee13801e18825d661e5005ae599460bc3a` 和固定 KWOK
`099ce5faf29193ac19f0d7529103327c48570f20`；逐个二进制 SHA 校验，
etcd 来源校验通过。全部在本机 loopback 的 18454/13581/13582 运行，
退出后这三个端口均无监听，不干扰共享后端的 18383 租约观察。

日志并非零错误：Deployment 状态更新有两次 409 冲突重试；启动阶段还有
readyz 500、引导资源 404 和 namespace 409。最终对象及写入者审计通过，
不能据此隐去启动重试或扩大为所有错误路径覆盖。

私有证据：`/root/.local/state/kubebrain/controlplane-replacement-reference.YFauJo6k/`，
子目录 `controlplane-reference.AjnRQ9W9`；runner.sha256 记录实际运行脚本，
result.json、replacement-state-check.json、replacement-audit-check.json
及完整审计/对象快照保留。PKI 和 kubeconfig 不入仓库。

下一步：新增夹具 CI 验证后，在共享后端满足新鲜零租约准入、身份校验及
恢复约束时运行 KubeBrain 对照。原 742e8b8c 的 60 秒清理失败不变。

## 真实错误 UID 前置条件实验

11:06:44.584367 UTC，另建一套本机参考控制面，仅在私有 PATH 的 curl 包装器
中把该夹具 DELETE 的 UID 前置条件改为
`11111111-1111-4111-8111-111111111111`。包装器限制固定 loopback 18455、
`controlplane-smoke/pods/chain-*` 路径及本轮 DeleteOptions 文件；原源码和
原始请求记录不改写。真实 apiserver 返回 HTTP 409/Conflict，指出请求 UID
与对象 UID `665daf69-2aba-4af3-91bd-bf5910a350ff` 不符。

这是期望失败实验：session 81325 终态 22，子结果
`operation_exit=22, backend_cleanup_exit=0, runner_exit=22`。审计仅有这一次
Pod DELETE 409，没有成功删除；脚本未继续生成 replacement-pods.json，
也没有把失败报告为替换成功。不是新一次正向替换通过或 KubeBrain 验收。

退出前观察到的五个 timeout supervisor PID（37590、37626、37876、37877、
38353）退出后均不存在，18455/13583/13584 均无监听。脚本 EXIT 路径逐个
终止并 wait 自有 supervisor；本次证明这种失败路径已收尾，不代表 SIGKILL、
宿主机崩溃等所有退出场景。共享集群和现有租约观察未改动。

证据：`/root/.local/state/kubebrain/replacement-precondition-reference.CNDYFeVu/`，
子目录 `controlplane-reference.jImT68lb`，包含注入包装器及其 SHA、原始和
注入 DeleteOptions、409 响应、审计及退出结果。此项发生于同一固定
`4ada3d15` 源码上，没有触发额外 CI 或产品镜像构建。
