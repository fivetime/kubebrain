# 参考 etcd 隔离控制面调度验证

2026-09-17，`hack/scale-lab/controlplane-reference-smoke.sh` 在本机独立 etcd 上完成
真实 apiserver → controller-manager → scheduler 联动。**这验证测试链路，不证明
KubeBrain 通过完整 Kubernetes 控制面验收，也不替代 KWOK、kubelet 或容器运行。**

## 实际结果

最终执行 session 65814 退出 0；证据目录
`/root/.local/state/kubebrain/controlplane-reference.qRQJJGYf/`。
apiserver、controller-manager、scheduler 均为已校验 SHA-256 的官方 v1.36.1，
参考 etcd 源码 `5cd9f4ee13801e18825d661e5005ae599460bc3a`，二进制 SHA-256
`e5c5c200f9daedf3851bdc8a22a7b394d2cf039c631bcb5fde6102a36f1aa3b7`。

apiserver 启用 Node/RBAC、禁用匿名认证。两个组件使用独立客户端证书，实际
`auth whoami` 匹配内置组件身份、不含 system:masters，创建 Deployment 的授权检查
均返回 no。controller-manager 使用独立控制器 ServiceAccount 凭据，启用
Deployment、ReplicaSet、ServiceAccount 及 token 四类控制器；两个组件均开启 leader election，
已保存各自 Lease holder。没有声称所有控制器或多实例选主故障均已验证。

管理员只创建一份 replicas=3 的 Deployment；一个 ReplicaSet 和三个 Pod 由真实控制器
创建。UID ownerReference 链、Deployment observedGeneration、PodScheduled=True 及
节点绑定检查通过。审计验证三个实际 Pod 的 name/UID 与 ReplicaSet 控制器创建响应
一致，绑定者为 system:kube-scheduler；ReplicaSet 创建者为 deployment-controller。
不使用直接注入派生对象的 derive 模式。

节点是人为设置 Ready、容量并精确移除初始 not-ready 污点的调度 fixture，不运行
node-lifecycle controller、KWOK 或 kubelet。三个 Pod 的最终 phase 都是 Pending，
没有实际拉取 pause 镜像，不能称 Deployment Available 或 Pod Running。

## 两次前置失败

- `controlplane-reference.rxE00GMi`，session 62552 退出 1：控制器创建 Pod，但模拟
  节点保留初始 not-ready/NoSchedule 污点，调度等待超过原 120 秒窗口。后续修复
  仅对该初始污点执行 UID/resourceVersion/原值条件检查后的移除，没有手动绑定 Pod。
- `controlplane-reference.SmrsFa1K`，session 97648 退出 1：对象链及调度通过，但
  Metadata 审计的 generateName 创建请求没有 objectRef.name，旧审计核对失败。
  之后仅对测试命名空间的 ReplicaSet/Pod 创建采集 RequestResponse，以返回对象
  身份核对。其他资源仍为 Metadata，不采集 ServiceAccount token 响应。

两次失败记录保留，不追溯改写为成功。正式最终运行在上述两次终态和端口释放后开始，
使用新的私有目录、CA 和独立 etcd 数据目录。

## 生命周期与范围

每次仅绑定本机 18453、13579、13580；不使用现有 6443，不访问管理集群、
KubeBrain 或共享 TiKV。启动前核验二进制摘要、版本、etcd 来源、互异端口、
监听冲突和文件锁。需显式 `ALLOW_LOCAL_CONTROLPLANE_TEST=true`。
子进程由专属 timeout supervisor 限时 300 秒，超时终止宽限 5 秒；最终按
scheduler、controller-manager、apiserver、etcd 逆序停止并回收。没有 systemd 修改、
按进程名称批量终止或复用既有 kubeconfig。

最终检查：上述三个端口无监听，最终运行对应组件进程均不存在，`result.exit=0`。
日志、审计、对象快照、PKI 和 etcd 数据留在属主私有目录，未上传仓库；证书一天有效。
保留数据用于诊断，不称为已擦除全部临时数据。实际执行脚本摘要在 `runner.sha256`。

本地 admission 四场景和审计八场景 race 测试通过（1.189s），包含禁止默认执行、
错误摘要、端口重叠/6443 拒绝，以及遗漏/重复 Pod、管理员代创建、错误 UID/绑定者、
失败请求和缺失响应体拒绝。它们不替代所有生命周期故障测试。

后续仍需为同一控制面工作负载接入独立 TiKV/PD 上的 KubeBrain，建立共享后端的
身份、独占前缀、租约及清理约束；再推进更多控制器、故障、持续负载与规模验收。
