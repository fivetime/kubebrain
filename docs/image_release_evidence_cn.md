# DBaaS 镜像结构化发布证据

`image.yml` 在已发布镜像校验和 dbaas 标签推广成功之后生成证据，再上传
名为 `dbaas-release-RUN_ID-RUN_ATTEMPT` 的 Actions artifact，保留 30 天。
两个步骤均遵循默认成功条件，不使用 `always()` 或忽略失败；缺失文件或
上传失败使工作流失败。没有更改现有双架构二进制、kubectl、标签和镜像
校验门限。

产物包含：

- `index.json`：此前实际校验过的 OCI 索引原始字节，不重新格式化。
- `release.json`：版本 1，固定仓库/工作流、完整源码 SHA、运行 ID、尝试
  编号、不可变镜像引用、索引 SHA256 和 linux/amd64、linux/arm64 摘要。
  运行 ID 和尝试编号保持十进制字符串，避免 JSON 数字精度丢失。

生成器 `build/image-release-evidence.sh` 校验索引字节摘要与镜像一致，
复用已有平台摘要选择器，拒绝相同架构子摘要、缺失平台、标签镜像、错误
源码或仓库、非法运行编号、符号链接以及超过 1 MiB 的输入。

这只是发布证据格式，不能单独当作准入授权。消费方必须通过认证的 GitHub
API 确认仓库、预期工作流、head SHA、run/attempt 和最终 success，并
验证下载 artifact 属于该次运行；还须比对载荷字段及索引字节摘要。
不能接受任意本地 JSON 自报 CI 成功，也不能把排队/执行中的工作流视作
已获准。已过期或缺失 artifact 必须显式处理，不能默默使用可变标签。
这些证据仍不替代实际 Pod/Node/容器身份、TLS 和原 30 秒故障验收。

本地验证：`go test -race -count=1 -timeout=2m ./build` 通过（2.758 秒），
`bash -n build/image-release-evidence.sh` 和 `git diff --check` 通过。
测试执行实际 Bash/jq，覆盖拒绝场景和精确 ID；工作流测试锁定生成/上传
顺序、成功条件和固定 upload-artifact 版本。测试命名以 `TestImagePlatform`
开头，由现有镜像工作流的构建前测试选择器覆盖。

记录时此变更仅在本地；运行中的 `0a0b0dfd` CI 不包含它，历史成功构建
也没有此 artifact。完整实验 CLI 对 artifact 的认证下载和消费仍待接入。

## 读取校验

`imageprepull.ParseReleaseEvidence` 读取不超过 32 KiB 的严格 UTF-8 JSON，
拒绝未知、重复、大小写别名字段和尾随 JSON。调用方必须另外提供从已
认证 CI/产物归属得到的 `ReleaseIdentity`；解析器逐项核对源码、运行
ID、attempt、固定仓库/工作流、镜像及索引摘要，再调用既有
`ApprovedRuntimeDigests` 校验原始索引与双架构摘要。不一致时不返回
部分已解析结果。ID 全程保留字符串，不经过浮点数。

该接口没有网络访问，不能自行确认工作流成功、artifact 归属或源码
可信度，也不把 `ReleaseIdentity` 类型本身当作认证。认证下载与完整
CLI 接入仍待完成。

测试实际执行 Bash 生成器并由 Go 解析结果，覆盖超大整数 ID 精度，另有
18 类错误输入反例。镜像预拉取整包竞态测试通过（4.946 秒），同包
`go vet`、`git diff --check` 通过；未操作集群。

## GitHub 运行身份查询

`VerifyGitHubReleaseRun` 通过已准入的 gh 可执行文件和凭据目录，对固定
`github.com` 执行一次 GET，路径固定为仓库中指定 run 的指定 attempt。
它要求有界上下文，单次最多 20 秒、不重试；前后检查工具/凭据准入，
响应及错误由强制留存回调保存，进程输出上限 1 MiB。环境不继承 GH_HOST、
GH_TOKEN、调试输出或 shell 启动钩子。

核对仓库及 head repository 的固定 ID `1285006877` 和全名、dbaas
分支、push/workflow_dispatch 事件、image.yml 路径、完整源码、run、
attempt 及 completed/success。字段依据认证 gh 对已成功运行
`35490241957/attempts/1` 的实际只读查询核实。API 可以增加无关字段，
但身份字段不接受大小写别名或重复字段。

该函数只核验运行元数据，不下载 artifact，也不证明 workflow 内容、
artifact 所属或镜像来源。工具与凭据目录仍需独立准入，留存回调也必须
提供真正持久化；完整认证下载和 CLI 仍未接完。

测试覆盖身份字段不符、重复/别名/尾随 JSON，并以实际子进程夹具核对
固定 GET argv、环境隔离、进程失败、留存失败和读后准入失败。整包竞态
通过（4.985 秒），同包 `go vet` 与差异检查通过，未操作集群。

同期镜像 CI `35491608335` 已成功，对应源码
`0a0b0dfd01f19a0e5dbebda65ebb37609d65764f`，终态和日志归档在
`/root/.local/state/kubebrain/ci-35491608335-terminal.txA9VgeD`。
该源码的回归 CI `35491608346` 仍为失败（断言问题详见事务边界对照
记录）；不能将仅镜像成功当作候选版本完整准入。后续修复需重新验证。

## 下载字节与 ZIP 内容边界

`ParseReleaseArchive` 接收最多 2 MiB 的 ZIP 字节及来自已认证产物元数据
的 SHA256。先核对整个归档摘要，只允许恰好两个普通文件：`index.json`
（最多 1 MiB）和 `release.json`（最多 32 KiB）。拒绝其他路径、目录、
符号链接、重复成员、超限内容、大小不一致或 ZIP CRC 错误，随后调用严格
发布证据解析器。只在内存中读取，不将成员路径解压到文件系统；失败时不
返回部分内容。

只读查询并下载现有构建 artifact `10599169857`，确认其 GitHub
`digest` 与下载 ZIP 原始字节的 SHA256 相同。元数据、ZIP 和摘要清单在
`/root/.local/state/kubebrain/artifact-digest-check.zMhji1h2`。该样本是
Docker build 记录，不是新发布证据，不能用于候选镜像准入。观察到的
`workflow_run` 含 run ID、两个 repository ID、head branch 和 head SHA；
新 artifact 的认证获取仍需核对这些归属字段及期望名称/attempt。

ZIP 解析测试覆盖摘要错误、路径穿越、缺失/额外/重复文件、符号链接、
超限成员、索引改动、CRC 损坏和非 ZIP 输入。整包竞态测试通过
（5.030 秒），`go vet` 与差异检查通过。本轮没有向 GitHub 或集群写入
状态；新增代码仅本地提交，仍未完成认证下载与完整实验 CLI。

## 认证下载编排

`FetchGitHubReleaseEvidence` 现把上述检查串为五次有界、只读、不重试的
请求：run-before → artifact-before → archive → artifact-after → run-after。
使用显式 artifact ID，不按 latest 标签回退。artifact 元数据必须匹配
固定仓库及 head repository ID、源码、dbaas 分支、run ID、含 attempt
的预期名称，明确未过期，并提供合法摘要和不超过 2 MiB 的大小。随后
下载原始 ZIP，核对大小/摘要及内容，最后复查元数据未变、运行仍成功。
现有真实样本的 size_in_bytes 也已与下载 ZIP 字节数核对一致。

所有请求复用固定 github.com/GET、独立凭据目录、受控环境和进程组
取消逻辑，每次最多 20 秒，外层最多 5 分钟。每阶段强制来源准入及完整
响应留存；最终检查之前不返回候选证据，任何错误都返回空结果。
ZIP 留存回调需支持最多 2 MiB，不能误用只允许 1 MiB 的日志接口。

测试以真实子进程夹具返回 API 元数据和 ZIP，核对五阶段顺序、十次前后
准入，以及初始运行拒绝、过期产物、归档不符、读后元数据/运行变化、
留存失败和读后准入失败的提前终止；每阶段仅一个请求，不重试。另覆盖
十种归属字段缺失及大小写别名。最终整包竞态通过（5.234 秒），同包
`go vet` 与差异检查通过。

这是下载编排实现及本地夹具测试，不是新 CI artifact 的真实端到端通过。
调用方仍须批准 workflow 源码、固定工具/凭据、实现持久化并核对同源码
回归 CI；不因镜像工作流成功而忽略回归失败。完整 CLI 和真实集群故障
验收仍未完成，本轮没有集群写入或推送打断正在运行的 CI。

## 下载响应持久化

`RetainReleaseResponse` 提供真实留存回调，要求调用方事先创建当前用户
所有的 0700 规范绝对目录。阶段名限于上述五种；元数据响应最多 1 MiB，
ZIP 最多 2 MiB，错误文本最多 64 KiB。输出以 JSON/base64 无损保存，
包含阶段、时间和观察错误。观察失败不是留存失败，下载器仍须单独拒绝。

每阶段独占创建 `github-STAGE.json`（0600、不跟随最终符号链接），
同步文件及目录后复核目录身份；拒绝覆盖、复用和不安全路径，失败不删除
部分记录。目录句柄只固定单次留存期间，调用方仍须在准入回调中固定整个
尝试的目录身份，且不能把成功留存当作下载或实验成功。

下载集成测试现使用该真实留存器。首轮因原有夹具目录不是 0700 而被
正确拒绝，已仅将夹具显式设为 0700，未放宽留存要求。补充 2 MiB 二进制
往返、重复写不覆盖、目录/文件符号链接、不安全阶段和大小边界测试后，
整包竞态通过（5.774 秒），`go vet` 和差异检查通过。真实 artifact
端到端验证及完整 CLI 仍未完成，本轮没有集群写入。

## 同源码回归 CI 强制门限

下载入口现在强制接收独立的 `RegressionIdentity`（run ID、attempt），
不再把回归 CI 检查留给调用方自行选择。回归源码只能取自镜像的
`ReleaseIdentity.Source`，不能另外传入一个更旧但成功的源码。运行 ID
必须与镜像工作流不同；非法或缺失标识在发起请求前拒绝。

顺序现为 regression-before → run-before → artifact-before → archive →
artifact-after → run-after → regression-after。新增的两次认证 GET 固定
检查 `.github/workflows/probe-regression.yml`、dbaas 分支、仓库及 head
仓库身份、精确源码/run/attempt 和 completed/success。复用原有有界
请求、工具准入与持久化；最后的回归检查失败也不返回部分候选结果。
调用方仍须审查并固定两个 workflow 的源码，成功运行元数据不替代该审查。

子进程集成测试验证七阶段顺序、十四次工具准入、回归失败、源码不符、
错误工作流、下载后回归状态变化，以及非法标识零请求拒绝。失败响应
也通过真实留存器保存。最终整包竞态通过（5.982 秒），`go vet` 与
`git diff --check` 通过。这是本地实现验证，不是新 artifact 的真实下载
验收，更不是 30 秒真实故障验收。当前两项 CI 仍在运行，未推送打断，
未修改测试集群。

## 67e4329f 回归 CI 终态

回归运行 `35492861895` 已完成且成功，精确源码为
`67e4329ff3cd15b3bac5fc3642e9e6d81501c030`。已核对所有步骤均成功，
包括此前失败过的 etcd service/Watch 回归，以及最后的全部探针竞态测试。
原始运行元数据与完整日志归档于
`/root/.local/state/kubebrain/ci-35492861895-terminal.dza9FkTp`，
`SHA256SUMS` 两项均校验通过。

该结果只覆盖上述源码，不覆盖后续尚未推送的本地提交。同源码镜像运行
`35492861896` 此时仍在 Build and push TiKV test image 阶段，不能据此
批准新镜像部署。真实发布产物验证、完整故障 CLI 及原 30 秒集群验收
仍未完成。

## 67e4329f 镜像成功与首份真实发布产物

镜像运行 `35492861896` 已完成且成功，与成功回归 `35492861895` 同为
源码 `67e4329ff3cd15b3bac5fc3642e9e6d81501c030`。运行元数据、完整日志及
artifact 列表在 `/root/.local/state/kubebrain/ci-35492861896-terminal.DdNpMFrx`，
三个文件的 SHA256SUMS 均校验通过。

通过已登录 gh 的只读 API 下载首份真实发布产物 `10600181944`，名称
`dbaas-release-35492861896-1`。元数据与原始 ZIP 保存在
`/root/.local/state/kubebrain/release-10600181944.02LkqvlH`。ZIP 大小
1041 字节，与 API 一致，摘要为
`sha256:4047c298e09256f2f5dae331d744f27f99c534c5a9a77bc881791fdb84f52210`，
与 API 及上传日志均一致；仓库/head 仓库 ID、分支、源码、run 归属匹配，
未过期。归档恰好包含 index.json 和 release.json，CRC 检查通过。

索引原始字节 SHA256、receipt 中镜像和 CI 验证日志一致：
`ghcr.io/fivetime/kubebrain@sha256:9c052ce86fdbe7c16847049b09cb54d15a07e12fb5ba2822ac162a006fa72382`。
receipt 的两个 runtime 平台摘要与索引唯一描述符匹配：

- linux/amd64：`sha256:997532304c8729d678e714e2c5c3acf07548b6b99bd5544b208cb2783d186ac4`
- linux/arm64：`sha256:2d53c587d2265de1e2bcbc79125499cbb63bbd23aae64ae631144889cc208a58`

本机没有 unzip，改用 Python 标准库 zipfile 在内存中检查，未解压到
文件系统或安装工具。本节是人工编排的真实只读产物核对，不声称已运行
Go 下载器七阶段认证/持久化流程，也不代表已部署或通过真实故障验收。

## 只读命令入口

现有 `image-prepull` 命令增加 `--mode=fetch-release`，搭配
`--release-plan=/absolute/private/plan.json` 和
`--release-plan-sha256=<独立审核的计划摘要>`。该模式不会创建 Kubernetes
客户端或修改集群；成功只输出经过认证的 receipt 与限定范围说明。

计划字段包括 source、image、run_id、run_attempt、regression_run_id、
regression_attempt、artifact_id、gh、config_directory、evidence_directory、
image_workflow、regression_workflow、files。两个 workflow 路径应指向从
指定源码提取并经人工审查的快照；**命令不会把快照哈希自动等同于来源审核**。
files 必须恰好固定五个不同文件：gh 可执行文件、独立凭据目录中的
hosts.yml/config.yml、两个 workflow 快照。计划必须为私有普通文件，
JSON 拒绝未知和重复字段。配置目录、证据目录必须分离且权限为 0700。
应使用新的证据目录，不复用既有尝试。

命令固定这两个目录身份，在每次请求前后重查目录、计划和五个文件摘要；
凭据文件要求私有，不输出凭据内容。外层总时限 3 分钟，响应通过真实
留存器保存到七个独占创建的阶段文件。失败保留部分证据且不输出候选
结果；调用方仍需确保所有父目录可信，独立审核 workflow 与源码关系，
不能仅自行计算一组哈希便宣布准入。

完整命令子进程夹具覆盖七阶段成功、错误计划哈希、workflow 改动、
凭据权限不安全、回归失败和目录复用拒绝。命令包竞态测试通过
（6.633 秒），库包竞态通过（5.986 秒）。这是命令夹具验证，尚未通过
该入口访问真实 GitHub。完整故障实验 CLI 及 30 秒真实验收仍未完成。

## 只读入口真实 GitHub 验证

使用本地命令源码 `663d2a34` 对已通过两项 CI 的 `67e4329f` 发布执行
`fetch-release`，进程退出 0。尝试目录为
`/root/.local/state/kubebrain/release-cli-67e4329f.ih7zBwQj`。
其中 result.json 输出精确固定镜像与源码，七个阶段文件均存在且观察错误
为空，stderr.log 为空；计划、两个工作流快照、结果、stderr 及七阶段
文件的 SHA256SUMS 均通过。ZIP 使用 base64 无损留存，不能用解码为
UTF-8 后的字符串长度衡量二进制大小。

运行前从精确 Git 提交提取两个 workflow，核对 checkout/revision、
构建和镜像验证、成功后发布证据的顺序，以及回归实际测试命令；与当前
工作树版本无差异。固定 gh 可执行文件及独立 0700 凭据目录内配置的
摘要，在七次真实认证 API 请求前后重新检查。凭据副本仅位于私有本地
配置目录，不写入仓库、日志或文档。此结果证明实际下载流程可用，仍不
代表候选部署或真实故障验收，也不覆盖后续源码。

与此同时，新源码 `4ce4dc16` 的回归 CI `35494231418` 失败，证据在
`/root/.local/state/kubebrain/ci-35494231418-terminal.zR8MqVk4`，原始日志
与元数据摘要均通过。失败为 `TestClientAuthPrivilegedMaintenanceAuthorization`
在 auth_client_test.go:1966 收到 DeadlineExceeded，需进一步复现定位；
不能将其归因为 Runner 或直接重跑宣布通过。该源码镜像 CI 仍在运行，
不准入该源码候选。

后续保持原测试时限不变，在本机运行该鉴权用例的竞态测试三次，全部
通过（整次命令 2.014 秒）。再运行整个 `pkg/server/etcd` 包（与 CI
非竞态阶段相同的 count=1、timeout=10m），退出 0，包耗时 149.671 秒；
目标用例在完整顺序中耗时约 0.03 秒。原始 JSON 事件、stderr、退出码
和校验清单在 `/root/.local/state/kubebrain/auth-maintenance-full.ccdLoOTa`。
该结果未复现 CI 超时，不意味着已经修复；未修改测试或产品代码。

为下一次 CI 保留更精确的定位信息，鉴权测试随后增加快照阶段 elapsed、
remaining 和 context 状态日志，不输出鉴权或快照数据，不改变共享 5 秒
截止时间、调用次序、断言或重试行为。三次定向竞态测试通过（2.020 秒），
`go vet ./pkg/server/etcd` 通过；这是诊断增强，不声称已修复超时。

真实下载还发现 gh 在不继承 HOME 时，会在调用方工作目录下生成
`.local/state/gh/device-id`。该文件已移至上述私有尝试目录的 gh-device-id，
仓库内仅清理由它产生的空目录，未删除原始状态。请求子进程现在将工作
目录固定为已准入的配置目录；夹具明确断言 PWD 与 GH_CONFIG_DIR 相同。
库包与命令包竞态分别通过（6.067、6.649 秒），相关 `go vet` 与差异检查
通过。未对 GitHub 或集群作写操作，仍未通过新源码回归准入。

镜像运行 `35494231450` 随后完成且成功，源码为
`4ce4dc16e2276f1f5d79149620f136dd890560b2`。原始元数据及完整日志在
`/root/.local/state/kubebrain/ci-35494231450-terminal.FmM3mgWa`，两项
SHA256SUMS 均通过。因同源码回归 `35494231418` 失败，不批准该候选部署；
后续推送包含真实下载入口和测试诊断增强，不能将其描述为已修复 CI 超时。

## bf4f1a28 回归结果

回归 `35495587386` 已 completed/success，精确源码
`bf4f1a28be48ec8646f533afa7f09b20c2ec53c8`。原始运行元数据与完整日志在
`/root/.local/state/kubebrain/ci-35495587386-terminal.XJPdB36X`，摘要校验
通过。新增日志显示，鉴权用例的快照阶段至测试返回分别约 69 毫秒和
186 毫秒，共享 5 秒预算分别还剩约 4.919 秒和 4.716 秒，context 均无
错误。该轮未复现旧超时；没有放宽截止时间，也不能反推旧失败根因已解决。
同源码镜像 `35495594714` 此时仍在运行，尚未开展新候选部署或故障实验。
