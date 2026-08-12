# 可选 Envoy gRPC 入口

此 profile 为 plaintext KubeBrain client 端口提供独立的 Envoy HTTP/2/gRPC L7 入口。它不会修改或替换
`deploy/production/kubebrain.yaml` 的默认直连 `kubebrain-client` Service；平台在完成容量、TLS 与网络边界审核后，
可显式选择部署：

```bash
kubectl apply -k deploy/production/envoy
```

客户端入口为 `kubebrain-envoy.kubebrain-system.svc:2379`。`kubebrain-envoy-upstream` 是 Ready-only headless
Service，Envoy `STRICT_DNS` 会看到每个 KubeBrain Pod IP，而不是把流量再次交给一个 ClusterIP 做不透明的二次
负载均衡。

关键约束：

- downstream 与 upstream 都启用 HTTP/2；route timeout、stream idle timeout 和 upstream connection idle
  timeout 均关闭，不能截断合法的 Watch 与 LeaseKeepAlive 长流；
- Envoy HTTP/2 schema 的并发 stream 最大值为 `2147483647`，低于 KubeBrain/etcd 的 uint32 最大值；生产容量
  仍以 KubeBrain `--max-watches=10000` 和 Envoy `max_requests=20000` 的较小业务上限为准；
- 三副本 Envoy 使用 PDB、hostname 反亲和、非 root/read-only 容器、digest-pinned 镜像和显式 NetworkPolicy；
- admin `:9901` 仅供带 `dbaas.kubebrain.io/monitoring-access=true` 的 namespace 访问，不是租户入口；
- 此 profile 当前只覆盖 plaintext。不要在公网或跨不可信网络直接部署；mTLS termination/passthrough、SDS、
  云厂商 LB、跨 AZ、NAT/conntrack 与 underlay 故障仍需按平台选型另行验证。

发布门禁必须用固定 Envoy 版本执行：

```bash
envoy --mode validate -c deploy/production/envoy/bootstrap.yaml
kubectl kustomize deploy/production/envoy | kubectl apply --dry-run=client -f -
```

真实差分门禁使用 `TEST_SCOPE=envoy-plaintext`，会让同一 production bootstrap 分别代理三成员 reference etcd
和三副本 KubeBrain，覆盖 KV、Watch、LeaseKeepAlive、MemberList、Status、Envoy admin upstream counter，以及
Envoy 进程重启并把 upstream 从 replica 0 迁移到 replica 1 后的 Watch/KeepAlive 自动恢复。
