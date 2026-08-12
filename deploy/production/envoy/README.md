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
- 三副本 Envoy 使用 PDB、hostname 反亲和、非 root/read-only 容器、digest-pinned 镜像和显式 NetworkPolicy；
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
