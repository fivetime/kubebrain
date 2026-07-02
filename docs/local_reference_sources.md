# Local Reference Sources

本文件记录当前机器上用于推进 KubeBrain etcd 兼容性和 Kubernetes 集成验证的本地源码参考路径。

## Reference Order

1. `/root/etcd`

   官方 etcd 仓库源码。后续实现或校验 etcd v3 API 行为时，优先参考该路径，尤其是：

   - `server/etcdserver/api/v3rpc/key.go`：Range、Put、DeleteRange、Txn 请求校验。
   - `server/etcdserver/txn/txn.go`：Txn compare 和 apply 语义。
   - `api/v3rpc/rpctypes/error.go`：官方 gRPC 错误码和错误文本。
   - `client/v3`：官方 Go client option 到 protobuf request 的映射。

2. `/root/kubernetes`

   官方 Kubernetes 仓库源码。用于确认 kube-apiserver 和 apiserver storage layer 如何调用 etcd，以及不同 Kubernetes 版本对 etcd v3 API 的实际依赖。重点参考：

   - `staging/src/k8s.io/apiserver/pkg/storage/etcd3`
   - `staging/src/k8s.io/apiserver/pkg/storage`
   - `vendor/go.etcd.io/etcd`，仅作为 Kubernetes vendored etcd 快照参考。

## Notes

- 这些路径是当前开发机器上的本地参考源码，不是 KubeBrain 构建依赖。
- 需要判断 etcd API 标准行为时，优先查 `/root/etcd`。
- 需要判断 Kubernetes 是否依赖某个 etcd 行为时，再查 `/root/kubernetes`。
- 不要把这些本地路径写进生产部署配置。
