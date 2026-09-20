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
