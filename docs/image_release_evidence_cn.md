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
