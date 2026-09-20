# Range 请求校验顺序

对照本机 `/root/etcd/server/etcdserver/api/v3rpc/key.go` 的 `Range`、
`RangeStream`：非法请求必须在进入存储处理前返回，流式请求还应先拒绝
不支持的排序和 revision filter。

KubeBrain 的可串行化读回退入口原先先读取领导权和后端 checkpoint，
随后才校验请求。本次将公共入口校验提前，内部单次执行入口也保留校验；
RangeStream 共享同一个校验函数，保持空 key、非法排序枚举、不支持的
排序、revision filter 的错误优先级和原有状态码/消息。

`TestRangeValidationBeforeBackend` 使用没有 peers/backend 的服务器，
覆盖线性读与可串行化读、公共与内部入口，要求返回对应错误且不发送流式
响应。nil 请求额外作为本地调用健壮性测试，不宣称是 etcd 网络协议差异。

此修复不证明有效请求的故障恢复、性能或原 30 秒故障验收通过。

本地验证：新增用例通过；`go test -race ./pkg/server/etcd -run
'TestRange' -count=1` 通过（3.667 秒）；`go vet ./pkg/server/etcd`
和 `git diff --check` 通过。以上不是新镜像的 CI 或在线验收结果。

提交 `bb30bfad14eccb88793b14ef9605a3df74cb5958` 的补充验证：
`go test -race ./pkg/server/etcd -count=1` 全包通过，耗时 351.129 秒。
这是本地服务包回归结果，不替代镜像来源校验或真实集群故障验收。
