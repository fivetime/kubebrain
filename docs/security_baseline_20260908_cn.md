# 2026-09-08 客户端及产品安全基线升级

本轮是客户端分仓后的独立安全修复，不升级 TiKV/PD 服务端，也不等于生产验收完成。

## 固定版本与修复范围

- 产品根模块、五个嵌套工具模块、客户端、CI 和镜像构建统一使用 Go 1.26.8。
  Docker build stage 为 `golang:1.26.8-bookworm@sha256:9fdc884aacc3bec89b20ffc69f4bb369c78210e3e4f600387b5128b12c199f81`，已回读官方多架构索引。
- 独立客户端 commit `b5b63af1128266a51c1ee63a8abbcf6f23ef5a6b`，固定远端版本
  `v2.0.8-0.20260908172918-b5b63af11282`，模块校验和见[维护说明](tikv_client_maintenance_cn.md)。
  客户端 grpc v1.83.0、x/net v0.57.0、etcd api/client/client-pkg v3.5.33；最低 Go 版本因依赖要求提高到 1.25.0。
- 产品 etcd api/client/client-pkg/etcdutl/pkg/server 六个模块为 v3.7.1；bigstream 的三个 etcd 模块同样升级。
  产品 x/crypto 为 v0.56.0，bigstream 的 x/net/x/text 为 v0.57.0/v0.40.0。
- `/root/etcd` 保持原始 clean commit `5cd9f4ee13801e18825d661e5005ae599460bc3a`。
  etcd-client-compat 的本地源码 replace 属于对标工具，不替换成发布版来规避检查；已单独扫描其实际依赖图。

初始客户端 RED 扫描命中六项可达问题：GO-2026-6091/6090/6089/5972、GO-2026-6061、GO-2024-2687。
产品 RED 扫描还命中 Go 标准库 GO-2026-6218/5026 和 etcd 的
[Watch 开放区间 RBAC 绕过](https://pkg.go.dev/vuln/GO-2026-6114)、
[TLS 握手无截止时间](https://pkg.go.dev/vuln/GO-2026-6107)。这些历史结果不删除、不改写成通过。
详细扫描额外发现的客户端旧 etcd、bigstream x/net/x/text、产品 x/crypto SSH 模块告警也已随本轮版本升级处理。

## 已验证结果

- 客户端提交前后：build、vet、全量单测、全量 race、模块校验和 govulncheck v1.6.0 均通过。
  [远端 CI 34257592017](https://github.com/fivetime/tikv-client-go/actions/runs/34257592017) 的 test/security 两个 job 均 success。
- 产品固定新远端客户端后：build、vet、除 `hack/production` 外全部根模块普通测试通过；
  `pkg/storage/tikv`、`cmd/option` race 通过。objectstore、bigstream、loadgen 的测试/vet 通过；loadgen 当前无测试用例。
- 产品自身已有 Watch FromKey/空 key/授权区间和 TLS 静默握手超时测试，普通运行及 race 连续三轮通过。
  这证明已覆盖的权限边界和连接行为，不将 etcd 库升级本身视为自研 Watch 实现的修复证明。
- build/workflow 回归、actionlint v1.7.12、相关 shell 语法检查和兼容工具的探针/参考版本回归通过。
  新增五个模块的工具链/etcd 固定版本回归；旧工具链放行和镜像缺少安全门禁均先复现失败，再验证修复通过。
- 客户端以及 objectstore、etcd-client-compat、bigstream、loadgen 最终扫描均无漏洞报告。
  产品根模块为 0 可达漏洞、0 已导入包漏洞，仍有 GO-2026-5932 的模块级告警：
  x/crypto 中未维护的 OpenPGP 包无修复版本，产品未导入该包。不抑制告警，也不宣称整个依赖模块零漏洞。
- `dbaas` 镜像工作流新增发布前强制扫描根模块、objectstore 和 kubectl 三个模块；
  任一步失败停止后续 build/push。还扫描 kubectl 双架构二进制，在提升 `:dbaas` 前逐字节比对镜像实际文件。

## 补齐 kubectl 二进制安全与兼容窗口

进一步核验发现官方 kubectl v1.36.2（与 Dockerfile 的 SHA-256 完全一致）使用 Go 1.26.4，
二进制扫描报告 18 项漏洞；同 minor 最新 v1.36.4 使用 Go 1.26.5，仍报告 9 项。
v1.37.0 amd64 候选扫描无可达漏洞，但超出已声明 Kubernetes v1.35 server 的
[官方 ±1 minor 窗口](https://kubernetes.io/releases/version-skew-policy/#kubectl)，因此未选用。
不能用源码扫描成功代表外部二进制安全，也不缩小项目支持窗口来取得绿灯。

新增独立 `hack/kubectl` 模块，使用官方 v0.36.4 模块的完整命令树、auth plugins 和启动处理，
升级 Go/依赖后重编译；没有内嵌另一套 Kubernetes 源码，也没有删减命令集。
版本为 `v1.36.4+kubebrain`，`gitTreeState=dirty`、gitCommit 指向产品源码，明确不是官方发行二进制。
新模块 vet、单测、race、Staticcheck、模块校验、源码扫描均通过；本地 amd64/arm64 二进制扫描均无漏洞报告。
完整命令入口、构建 metadata、未知架构拒绝、源码缓存层和发布扫描门禁均有回归测试。

本地验证构建使用基线产品 SHA `b2f4bed932a54fcf8c5c048bcd8adae3d3730e9b` 和测试 metadata
`2026-09-08T18:05:00Z`，dirty 标记保留。amd64 SHA-256 为
`8cbaaaaed2687ec448479385a43fe5a8407bf1684654e81a0bcf72d22a7e0f18`，arm64 为
`5c40e8041bfc619b3a4d22df3b64864d94eaf0d49cb1951457bd8b883afb6872`。
重复本地构建字节相同。amd64 工具成功读取授权集群 namespace UID、通过 exec 查询同一 PD cluster ID，
并完成 server-side dry-run，确认未创建实际 Pod。API `/version` 实测为 v1.36.1，和此前节点 kubelet
v1.36.0 记录区分；未升级用户集群或替换本机全局 kubectl。
扫描与版本记录保存在环境材料目录 `security-kubectl-*`。使用固定 Go Docker stage 对两个架构
交叉编译，产物与本地扫描二进制逐字节一致，ELF machine 分别为 X86-64/AArch64；该验证
使用隔离 kubectl 构建入口，不等于完整产品镜像已构建或部署。
新 kubectl 的完整 Kubernetes v1.35/v1.36 矩阵及原生 arm64 运行仍待后续验证。
临时下载的官方候选工具、本地及容器构建输出已清理；日志和校验和记录保留，产物可从固定模块重建。

## 真实 TiKV 复测与清理

在用户授权的 `tk-001-003` / `kubebrain-dbaas-test`，重新核验 namespace UID、PD cluster ID
`7683177044639569228` 与 rook-ceph 消费者 StorageClass 后，用 Go 1.26.8 和新远端客户端编译
`TestLargeKeyRoundTripTiKV`。worker3 的专属 Pod 完成 600 KiB/2 MiB 物理 key 的 Put/Get/Iter
逐字节 round-trip，1.45 秒 PASS，2026-09-08 17:36:21 UTC 正常退出，未 skip。

Pod UID `743095a9-d977-41f5-954a-896f906ed101`，测试二进制 SHA-256
`d955b208da1d70aeca0e1fa8a420f0543f3a95c095ad31e90a12c8b3ef5f31a5`。
用例清理自己的测试前缀；临时 Pod 已按 UID 删除，本机临时测试二进制及 checksum 文件已清理。
未新增 PVC，未操作 rook-ceph-secondary。日志、终态 JSON 和清单保存在仓库外
`/root/.local/state/kubebrain/tk-001-003/client-security-large-key*`。

## 尚未完成的门禁

产品提交前的 `test-shard.sh --verify 4` 已确认 703 项完整分配为 170/193/180/160。
首轮分片 0/1/3 通过（259.349/466.208/736.058 秒），分片 2 失败：`TestValidateTiKVRegionHealth` 的非法 PVC 容量断言
预期容量格式错误，却提前收到 `cannot read PD pending-peer response`。测试使用 1 秒单请求
timeout，产品默认 10 秒；尚不能仅凭这一差异认定根因。原样独立连续三次复测全部通过
（277.238 秒），未放宽超时或断言。第二轮完整 verifier/四分片全部通过
（275.473/469.110/326.519/742.669 秒）。随后因补齐 kubectl 二进制修复，对最终变更执行第三轮
完整 verifier/四分片，703 项全部通过（258.426/452.167/308.982/737.011 秒）。
首轮非稳定失败仍保留为待观察项，不用后续通过抹去，也不声称已经证明其根因。
产品提交 `5645caa6b014c2c17b84af968f242efeee3a388a` 后立即原样执行 verifier 和四分片，
703 项全部通过（250.398/449.578/301.841/719.691 秒）。提交已快进推送；push 自动触发
[镜像 CI 34264830428](https://github.com/fivetime/kubebrain/actions/runs/34264830428)，
首次回读为 queued，尚不能据此认定发布成功；未重复手动派发。

完整根 Dockerfile 的 amd64/TiKV 本地预提交构建成功，镜像
`kubebrain:security-precommit-20260908`，本地 image ID
`sha256:61ef6a5c606901730ae2052759a92b4768fc1a5fbe1fb1d2ba2fb447377f89b8`。
非 root 用户 `65532:65532`、主程序 TiKV/Go 1.26.8/linux-amd64 与 OCI labels 均已回读。
该预提交镜像使用基线 SHA b2f4bed9 和明确的 `0.0.0-security-precommit` 标识，仅验证本地构建，
不是新产品提交的发布镜像，也没有部署到集群。构建日志保存在环境材料目录 `security-product-image-build.log`。
从未启动的临时容器提取实际镜像 `/usr/local/bin` 后，全部 66 个 Go 可执行文件均核验为
Go 1.26.8、amd64，逐个二进制漏洞扫描通过；模块级 OpenPGP 告警仍保留。
完整扫描与镜像身份记录为 `security-product-image-binary-scan.log` 和
`security-product-image-inspect.json`。提取容器、临时目录以及上述仅供验证的本地镜像
已定向删除，日志保留；未清理共享 Go/BuildKit 缓存。

产品 Staticcheck v0.7.0 本轮仍失败，输出包含 52 项诊断；在隔离临时源码目录使用同一
Go 1.26.8 对修改前 commit `b2f4bed9` 复测，输出逐行完全相同，确认是既有未清项。
前后日志保存在环境材料目录 `security-staticcheck-before.log` / `security-staticcheck-after.log`。
用于对照的临时源码目录已清理，可从 Git 恢复。
不得把 govulncheck 成功写成全部 CI 成功。objectstore 的 Staticcheck 已通过。
新安全基线发布镜像仍待 CI 完成，尚未部署；本地预提交构建不替代发布验证。
三副本 KubeBrain、恢复、监控及故障验收仍待后续推进。

## 已上传发布产物的独立核验

CI `34264830428` 仍报告 build/push in_progress 时，固定源码标签已可读取。以下核验
针对已上传的不可变产物，不把标签存在或本地验证成功当作 CI 已完成：

- 索引 `sha256:6ab33dc81572dfc318b02d4f840252111790690e1fa04d20af1f772db309a224`，
  amd64 runtime manifest `sha256:0f5cabecae924442cc011bdf562439908f3334c9958375d61c4b84cdc615d88e`，
  arm64 runtime manifest `sha256:1ce160482774e91040ad771715b634f22ff7b4e3947e0af78571bb361a8d1479`。
- 下载 amd64 后以 `--network none` 运行版本入口：`0.0.0-dbaas-5645caa6b014`、
  源码 `5645caa6b014c2c17b84af968f242efeee3a388a`、Go 1.26.8、TiKV、linux/amd64、
  构建时间 `2026-09-08T18:45:13Z`；OCI labels 与非 root `65532:65532` 一致。
- 镜像 kubectl 为 `v1.36.4+kubebrain`，Go 1.26.8、源码 SHA 与构建时间匹配。
- 提取镜像实际 `/usr/local/bin` 的 66 个 Go 可执行文件，逐个验证 Go 1.26.8、X86-64，
  govulncheck v1.6.0 二进制扫描全部通过，终态 `SCANNED_GO_BINARIES=66 PASS`。
  无可达漏洞不等于没有模块级告警，OpenPGP 告警仍保留。
  主程序 SHA-256 为 `8d4562fa5445a153d13e2c07a21e85f37c3bffac02742f2412df5183861af1e7`，
  kubectl 为 `0dc44d82cd8d6cce454784bc668f41769f2cff49d4c4438aeff27fefbcdfd51a`。

持久证据为环境材料目录中的 `security-release-image-index.json`、
`security-release-image-inspect.json`、`security-release-version.log`、
`security-release-kubectl-version.json`、`security-release-binary-scan.log`。
提取用未启动容器 `04fdd764537a0328a2b0aed4cb691f83f8cd887f23ab7d8888eb480e181276bb` 已删除。
本地下载镜像及 `/tmp/kubebrain-release-audit.rIYu3W` 暂留用于紧接着的部署核验，
其中 kubectl 已用于读取授权集群；未替换全局 kubectl，结束后需定向清理。
