# da303fdb 候选镜像审核

源码：`da303fdbd460ee42f3aa158fac27c396faf6b58f`。三项 CI 已核对相同 head SHA，
均为 completed/success：

- [镜像构建 35352626418](https://github.com/fivetime/kubebrain/actions/runs/35352626418)。
- [探针回归 35352626497](https://github.com/fivetime/kubebrain/actions/runs/35352626497)。
- [后端协议集成 35352626422](https://github.com/fivetime/kubebrain/actions/runs/35352626422)。

上次 `7fa4a6aa` 的 Watch 回归失败记录保留；此次候选包含回放旧进度标记
修复，对应 Watch 回归已在新的探针 CI 通过。没有把旧镜像 CI 成功移用
为本候选的测试证据。

## 独立审核结果

私有 owner `/root/.local/state/kubebrain/release-da303fdb.OAubJCfA` 的 audit.sh
一次性执行，49511 终态 0，claimed 已消耗，不得重跑。程序仅在本机
network=none、只读、drop ALL 的隔离容器中执行 version，没有部署集群。

| 清单 | SHA-256 |
| --- | --- |
| 多平台索引 | `4cf0eca4840fc55d090c62cc8684b0b3ed58c30c6a6b1b0d7d32100dfaae1e71` |
| linux/amd64 | `0484a6308fef13268edea2f033867a84323ead0a801fc6e7543c69ede3e522d1` |
| linux/arm64 | `60f9dfccf8809f1e37957c9551d9c6953cddfd69b5354eb7887fbb6bac79e018` |

固定镜像引用：
`ghcr.io/fivetime/kubebrain@sha256:4cf0eca4840fc55d090c62cc8684b0b3ed58c30c6a6b1b0d7d32100dfaae1e71`。

已核对索引原始字节摘要、两个平台、实际 amd64 二进制与 OCI 标签：
版本 `0.0.0-dbaas-da303fdbd460`，源码完整 SHA、TiKV、Go 1.26.8、linux/amd64、
BuildTime `2026-09-18T13:51:28Z`，镜像用户 65532:65532。提取二进制的
`go version -m` 显示预期的 fivetime TiKV client fork 版本与模块校验和、
grpc v1.83.2；不是只看标签或版本字符串。冻结的 lease.go 文件哈希仍为
`3c98f802359a5f185dc6e618691ad6098641a54afa528668c6dfcaf8091ccd88`。

证据与源码摘要检查通过。临时审核容器及提取出的二进制已删除，精确
owner 标签的容器查询为空；镜像缓存和源码、日志、清单证据保留。

## 尚未证明

`verified.json` 明确限定 `published_image_identity_only`、执行平台 amd64，
`deployment_acceptance_proven=false`。本轮未运行 arm64 程序，未验证三台
工作节点实际 imageID/版本，未进行诊断启用/恢复或原始 30 秒故障实验。
下一步应先做逐节点版本烟测，再用新 admission 进入专用集群实验。
本地完整工具包回归会话 8608 仍需独立收集终态，不能用 CI 成功代替。
