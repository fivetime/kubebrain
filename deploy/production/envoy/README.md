# 可选 Envoy gRPC 入口

此 profile 提供两个可选入口：`2379` 是面向 plaintext KubeBrain 后端的 HTTP/2/gRPC L7 入口，`2380` 是
面向 mTLS KubeBrain 后端的透明 TLS passthrough 入口。它不会修改或替换 `deploy/production/kubebrain.yaml`
的默认直连 `kubebrain-client` Service；平台在完成容量、TLS 与网络边界审核后，可显式选择部署：

```bash
kubectl apply -k deploy/production/envoy
```

明文客户端入口为 `kubebrain-envoy.kubebrain-system.svc:2379`；mTLS 客户端入口为
`kubebrain-envoy.kubebrain-system.svc:2380`。`kubebrain-envoy-upstream` 是 Ready-only headless Service，
Envoy `STRICT_DNS` 会看到每个 KubeBrain Pod IP，而不是把流量再次交给一个 ClusterIP 做不透明的二次负载均衡。

关键约束：

- downstream 与 upstream 都启用 HTTP/2；route timeout、stream idle timeout 和 upstream connection idle
  timeout 均关闭，不能截断合法的 Watch 与 LeaseKeepAlive 长流；
- Envoy HTTP/2 schema 的并发 stream 最大值为 `2147483647`，低于 KubeBrain/etcd 的 uint32 最大值；生产容量
  仍以 KubeBrain `--max-watches=10000` 和 Envoy `max_requests=20000` 的较小业务上限为准；
- 三副本 Envoy 使用 PDB，以及按同一 `pod-template-hash` 计算、要求至少三个非 tainted hostname 域的
  topology spread；`maxUnavailable=0/maxSurge=1` 允许 rollout 先增后减，最终同一 revision 仍回到三节点
  各一副本。容器为 non-root/read-only，镜像 digest 固定，并使用显式 NetworkPolicy；
- preStop 使用固定镜像中的 `/bin/bash` `/dev/tcp` 依次向 Envoy admin 发送显式
  `POST /healthcheck/fail` 与 `POST /drain_listeners?graceful`，逐项校验 HTTP 200；Envoy 以显式 5 秒
  `immediate` drain 在 10 秒 hook 宽限期内向已有 HTTP/2 stream 发起迁移。hook 在 readiness fail 后先等待
  5 秒供 EndpointSlice/kube-proxy 摘除旧地址，再 drain listener 并等待 5 秒；Deployment `minReadySeconds=20`，
  readiness 每秒检查且 `failureThreshold=3`，避免短暂 CPU 抖动同时摘除多个健康副本；主动 readiness fail
  仍会在 5 秒传播等待内生效。整个 hook 由 `/usr/bin/timeout` 限制为 20 秒，为固定 10 秒 sleep 外的 admin
  往返保留余量。Envoy admin 的 mutating endpoint 不接受 GET，固定官方镜像也不包含 curl；
- admin `:9901` 仅供带 `dbaas.kubebrain.io/monitoring-access=true` 的 namespace 访问，不是租户入口；
- `2380` 不终止或检查 TLS：客户端证书、服务端证书和 etcd 的证书 CN 身份均端到端保留，Envoy Pod 无需挂载
  租户 CA、证书或私钥；它不是 L7 路由，不能提供基于 RPC method 的策略；
- `2379` 不能直接暴露到不可信网络。若要求 Envoy 终止 mTLS/SDS，必须先定义不会丢失客户端证书身份的可信
  协议，不能用可伪造的转发 header 冒充 etcd 原生 TLS 身份；
- 云厂商 LB、跨 AZ、NAT/conntrack 与 underlay 故障仍需按平台选型另行验证。

发布门禁必须用固定 Envoy 版本执行：

```bash
envoy --mode validate -c deploy/production/envoy/bootstrap.yaml
kubectl kustomize deploy/production/envoy | kubectl apply --dry-run=client -f -
```

真实差分门禁使用 `TEST_SCOPE=envoy-plaintext`，会让同一 production bootstrap 分别代理三成员 reference etcd
和三副本 KubeBrain，覆盖 KV、Watch、LeaseKeepAlive、MemberList、Status、Envoy admin upstream counter，以及
Envoy 进程重启并把 upstream 从 replica 0 迁移到 replica 1 后的 Watch/KeepAlive 自动恢复。

mTLS passthrough 门禁使用同一 production bootstrap，并要求 reference etcd 与 KubeBrain direct endpoints 都
启用测试 CA：

```bash
ENVOY_BINARY=/path/to/envoy \
TEST_SCOPE=envoy-tls-passthrough \
TLS_CA_FILE=/path/to/ca.crt TLS_CERT_FILE=/path/to/tls.crt TLS_KEY_FILE=/path/to/tls.key \
TLS_SERVER_NAME=127.0.0.1 \
KUBEBRAIN_DIRECT_ENDPOINTS=127.0.0.1:34379,127.0.0.1:35379,127.0.0.1:36379 \
./hack/etcd-client-compat/run-direct-moveleader-differential.sh
```

该隔离门禁用同一 `TLS_CERT_FILE` 启动临时 reference server 并作为测试 client，因此证书必须包含
`serverAuth,clientAuth` 和 `TLS_SERVER_NAME` SAN；生产租户客户端仍应使用独立的 client-only certificate。

跨 Envoy replica 的滚动迁移门禁使用 `TEST_SCOPE=envoy-replica-drain`。它先在 replica 0 上建立 Watch 与
LeaseKeepAlive，再加入 replica 1、对旧 admin 执行与 production preStop 相同的 readiness fail 并停止旧进程；
两个长流必须从新地址恢复，lease 必须跨原 TTL 存活。

真实 Kubernetes 滚动门禁要求三副本 Envoy 已分别 Ready 在三个 node，并通过临时 NodePort 走 kube-proxy：

```bash
KUBE_CONTEXT=kind-kubebrain-envoy-k8s \
NAMESPACE=kubebrain-system \
NODE_HOST=172.18.0.11 \
./hack/etcd-client-compat/run-envoy-kubernetes-rollout.sh
```

`KUBE_CONTEXT` 必须显式指定；`NODE_HOST` 省略时使用集群首个 node 的 InternalIP。若 Kind 将 NodePort 映射到
另一个宿主端口，可同时指定 `NODE_PORT` 和 `NODE_ENDPOINT_PORT`。测试客户端来源必须被
`kubebrain-envoy-ingress` NetworkPolicy 准入。门禁会核对三个初始 Pod UID 与 Ready EndpointSlice target UID
完全相同。默认连续执行三轮 Deployment restart，共替换九个 Pod；每轮都要求三个旧 Pod 各自在仍存在时先退出
Ready EndpointSlice，且任何采样点都保留至少两个 Ready target。同一稳定 NodePort 上的 CreatedNotify Watch
和 TTL=5 LeaseKeepAlive 必须贯穿全部轮次，每轮结束都收到新的 Watch event、正 TTL keepalive，且附租约键
最终仍存在。测试还会建立最多 30 个独立 Watch client，读取每个旧 Envoy Pod admin 的
`http.kubebrain_downstream.downstream_cx_active`，只有三个副本都实际承载长期连接才开始 rollout。每个 cohort
client 都在同一 HTTP/2 connection 上建立独立 Watch 与 TTL=5 LeaseKeepAlive；两类 stream、各自附租约键和
lease ID 都必须逐轮恢复。`ROLLOUT_CYCLES` 可在 `[1,10]` 调整；临时 Service 会在退出时删除。

设置全部四个 `TLS_CA_FILE`、`TLS_CERT_FILE`、`TLS_KEY_FILE`、`TLS_SERVER_NAME` 后，同一门禁会把临时
NodePort 切到 `2380`，通过 clientv3 mTLS 走 Envoy opaque TCP passthrough。四项必须全设且三个文件可读，否则
fail closed；测试以每个 Pod 的 `listener.0.0.0.0_2380.downstream_cx_active` 证明 cohort 覆盖三个 TLS
listener。证书由 KubeBrain 端而非 Envoy 验证，Envoy 仍不挂载租户私钥。
门禁会在合法 cohort 建立后、rollout 前以及全部 rollout 后分别证明 plaintext 与缺少 client certificate 的
连接被拒绝；反向探针后还要求每个 Envoy 恢复三个健康 upstream 且无 active ejection。Kind host-port/NodePort
现场中 TTL=3 在连续三轮代理连接迁移时仍观察到真实租约过期，因此当前发布门禁不把 TTL=3 宣称为无损边界。
