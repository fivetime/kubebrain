# 实验性交接候选 02786d91 镜像审计

2026-09-18，精确源码 `02786d91ff406fe50d72e9baebdf2963b74b702c` 的
[镜像 CI 35337650967](https://github.com/fivetime/kubebrain/actions/runs/35337650967)
和 [探针 CI 35337650907](https://github.com/fivetime/kubebrain/actions/runs/35337650907)
均完成且成功。保存原日志，核对实验参数失败关闭和 CLI 身份文件策略双
Endpoint 用例 PASS。没有该 SHA 的真实 TiKV backend CI 记录，不能移用
其他 SHA 的成功结果；`backend_ci` 明确为 null。

## 固定身份

镜像仓库为 `ghcr.io/fivetime/kubebrain`：

| 对象 | 摘要 |
| --- | --- |
| OCI index | `sha256:0dee59a0b7575aa1e9f5a7586f74c9c19d22126becfd43046c29190a57aa8f1a` |
| linux/amd64 | `sha256:3acdfe87728e75b4efcd730f99c62606bcdf59281778fb6db81cd59a6c4fb1d5` |
| linux/arm64 | `sha256:d0080af5155cc3ecd085b25c0c677f448a0c2d24a3c9d6d2fa887102b901c39d` |

按精确源码标签解析后，重新计算原始 index 的 SHA-256，用该源码归档中的
平台选择脚本核对恰好两个目标架构。移动 dbaas 标签另行记录，不作为部署
身份。amd64 临时容器只执行 version，network=none、只读根目录、drop ALL、
no-new-privileges；未启动服务。

版本为 `0.0.0-dbaas-02786d91ff40`，Git SHA 与上述源码一致，Storage TiKV，
Go 1.26.8、linux/amd64、BuildTime `2026-09-18T11:03:01Z`，与 OCI 标签
匹配，镜像用户为 65532:65532。提取二进制读取 Go build info，核对
TiKV fork 为 `github.com/fivetime/tikv-client-go/v2`
`v2.0.8-0.20260909023231-832b70fd622f`，模块校验和
`h1:xmTt2n1e/Yy8Tq5cCn4MqQsTtzTQWMuKX2GAqDkJuJg=`，grpc v1.83.2。
临时容器和提取二进制已清理，保留镜像缓存。arm64 仅核对清单，未执行。

## 证据范围

owner 为 `/root/.local/state/kubebrain/release-02786d91.KACRR3gU/`，包含
原 CI 对象和日志、源码归档、descriptor/index、version/labels/buildinfo、
SHA-256 清单和清理日志。审计执行 86169 终态 0，`exit-code` 为 0，
`verified.json` 的范围为 `published_image_identity_only`，明确
`deployment_acceptance_proven:false`。

这不是服务升级或原 30 秒故障验收结果。接下来仍须按
[现场迁移和恢复约束](peer_retirement_cluster_transition_cn.md)验证独立成员
身份、新版本实际启动、scope 绑定、控制接口鉴权及原请求故障门限。

## 三节点执行证据及校验脚本失败

同日两次隔离版本探针均在 worker1/2/3 成功执行，三个 Pod 都为
Succeeded/exitCode 0；但两个驱动都以 1 结束，原因是要求运行时 imageID
必须为 amd64 manifest digest，实际返回上述已审计的 OCI index digest。
第二次即使 Pod spec 直接指定 amd64 digest，imageID 仍为 index。不能将
这两次驱动写成成功，亦不能声称运行时报告了 amd64 digest。

首次证据为 owner 下 `nodes.aBknaj7x`（执行 92755），未在摘要断言失败前
保存版本日志，因此它单独不足以证明版本。第二次 `nodes-recheck.pVaJ3mLI`
（执行 68125）在退出清理时保存了三节点日志。独立 `postflight.sh` 退出 0：
核对三 Pod 创建 UID、目标节点、exitCode、指定 amd64 镜像、返回的已审计
index，以及三份日志中的精确 Git SHA、Storage TiKV、Go 1.26.8 和
linux/amd64；保留两个原 exit-code=1，不覆盖原失败。这个复核是已保存
执行证据的身份检查，不是重新执行服务或故障实验。

两次均使用独立临时命名空间，先创建 ingress/egress 默认拒绝策略，
非 root、只读根目录、drop ALL、禁止提权，无数据卷、Secret 或 API token，
只执行 version，不连接 TiKV。镜像传输上限 600 秒不改变原故障门限。
清理使用 API 侧 namespace UID 前置条件；两个命名空间均确认不存在。
仅留下节点镜像缓存和本机证据，临时编译的删除助手已清理。

独立复核还确认三节点 UID 不变，原 kubebrain-local StatefulSet UID 和
完整 spec 不变，generation 44、3/3 Ready。当前没有部署候选服务镜像或
启用实验协议；不能据此宣称新服务启动、真实 TiKV 交接或生产就绪。
