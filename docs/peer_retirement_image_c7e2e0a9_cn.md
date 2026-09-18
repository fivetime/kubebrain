# 实验性 peer 交接候选镜像身份审计

2026-09-18，源码 `c7e2e0a9d035447488ac5a00b60f5502c426bebe`：

- [镜像 CI 35331815217](https://github.com/fivetime/kubebrain/actions/runs/35331815217) 完整成功，包括模块扫描、双架构构建、发布镜像验证、提升 dbaas 标签和收尾步骤。
- [probe CI 35331815236](https://github.com/fivetime/kubebrain/actions/runs/35331815236) 完整成功，包含新增 endpoint 生命周期步骤。
- 保存原日志并核对 PASS：CLI 格式身份的文件策略双 Endpoint 转发、实验参数加载失败关闭、CRL 到期及恢复。没有把后续本地新增的原 KeepAlive 流测试当作此 SHA 已运行的测试。
- 此 SHA 没有本轮真实 TiKV backend CI 记录，证据写 `backend_ci:null`，不把更早提交的 backend 成功移用到这里。

## 已冻结的镜像身份

| 项目 | 摘要 |
| --- | --- |
| OCI index | `sha256:ab75be8a79d4c25a41f53dfefcdf359e04890cdf1050a5ff5ea78241fee4bc11` |
| linux/amd64 | `sha256:9e02fc2d0fc1591609519250eedc3d3e28e5e30a017e3107d2055983d6b7b70b` |
| linux/arm64 | `sha256:88f9b79a69056ea44597f0ad1ad030308cbec9661e3669828420e44f7b6ffcfe` |

仓库为 ghcr.io/fivetime/kubebrain。读取不可变源码标签，再按摘要取原始
index，重新计算其 SHA-256，与 descriptor 一致；使用该源码版本内的
image-platform-digest.sh 选取架构，确认恰好 linux/amd64 和 linux/arm64。
审计时 dbaas 标签指向相同 index，但后续实验必须用摘要，不能依赖移动标签。

amd64 镜像拉取后，仅启动 network=none、readonly、drop ALL、no-new-privileges
的临时容器执行 version，不启动服务。核对 Git SHA、版本
`0.0.0-dbaas-c7e2e0a9d035`、Storage TiKV、Go 1.26.8、linux/amd64、BuildTime
2026-09-18T09:53:15Z 与 OCI 标签一致，镜像用户 65532:65532。

从同一容器提取二进制，只读取 Go build info 和 SHA-256：fork 依赖为
github.com/fivetime/tikv-client-go/v2 v2.0.8-0.20260909023231-832b70fd622f，
sum 为 `h1:xmTt2n1e/Yy8Tq5cCn4MqQsTtzTQWMuKX2GAqDkJuJg=`；grpc 为 v1.83.2。
提取的二进制和审计临时容器已精确清理，保留镜像缓存和证据，不做 Docker
全局清理。arm64 本轮只校验清单，不宣称在本机执行过 arm64 二进制。

## 证据与限制

owner：`/root/.local/state/kubebrain/release-c7e2e0a9.ZpUqLgka/`。
CI 证据在 ci-evidence.NV2qWJcc，包含两个原日志及 SHA-256、API run 对象和
明确范围的 verified.json。镜像证据在 image-evidence.B7PtlEpA，包含源码
归档校验、descriptor/index/platform、version、labels、buildinfo、binary
摘要与 cleanup 日志。执行 92640 终态 0，输出 PUBLISHED_IMAGE_IDENTITY_VERIFIED。

结论仅是 `published_image_identity_only`，并明确
`deployment_acceptance_proven:false`。没有修改 StatefulSet、导入实际 peer
Secret 或开始故障实验。还须按[迁移与恢复准备](peer_retirement_cluster_transition_cn.md)
执行现场独立身份接入、完整启动和原 30 秒故障门限验证。
