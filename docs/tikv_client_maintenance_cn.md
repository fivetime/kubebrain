# TiKV Go 客户端分仓维护

## 仓库与依赖边界

- 产品仓库：`fivetime/kubebrain`，当前开发分支 `dbaas`。
- 客户端仓库：[fivetime/tikv-client-go](https://github.com/fivetime/tikv-client-go/tree/kubebrain-v2.0.7)，维护分支 `kubebrain-v2.0.7`；`master` 保留上游跟踪用途，不作为产品的浮动依赖。
- 上游基线：`tikv/client-go` 的 `v2.0.7`，commit `c853ddc68c41ff9dad1ed8ce11c3f062a84bf220`。本次分仓不升级到最新上游，不修改 TiKV 服务端。
- 客户端模块名和业务 `import` 继续使用 `github.com/tikv/client-go/v2`；KubeBrain 的 `go.mod` 用远端 `replace` 选择 fork 的固定版本。
- 初始固定版本：`github.com/fivetime/tikv-client-go/v2 v2.0.8-0.20260908163401-c29dacb1d522`，commit `c29dacb1d522044477895af370a32878a1a4c44f`。该伪版本由 Go 根据上游 tag 和实际提交生成，不代表升级到了官方 v2.0.8。
- 模块校验和：`h1:TQ+L8uiCNBSKN5tiClE8N02+tE3jHDZszEJra6NlVWw=`；go.mod 校验和：`h1:HQFzF4oEfDDK4ffaGoBYsAfV/oJzVYESJGOgn25pHRA=`。均写入产品 `go.sum`。

客户端的权威[补丁清单及历史映射](https://github.com/fivetime/tikv-client-go/blob/kubebrain-v2.0.7/KUBEBRAIN_PATCH.md)在独立仓库维护，不能再在产品仓库复制一份客户端源码独立修改。

产品分仓提交为 `522da3334662200606d01e32dad8556238a3fc40`，已推送 `dbaas` 并触发 [镜像构建 34253661949](https://github.com/fivetime/kubebrain/actions/runs/34253661949)。该构建现已 success，双架构 digest 为 `sha256:1e1dd21b06c7cd0ff1fbfda04ea26d55f14e98949ca9333c0eff4f8fd12871cb`；它仍是安全升级前版本，不能当成下述新客户端和工具链的构建结果。

## 迁移证据

来源为产品 commit `507016642e298ebde5360339cd397dd48514e61d` 的 `third_party/tikv-client-go`，Git tree 为 `c436408c4d22db571d165609e0c3fc516c40f774`。

27 条客户端改动按顺序接回真实上游 v2.0.7 历史，保留作者、主题、补丁顺序，并在客户端文档记录原产品 commit 与新客户端 commit 的对应关系。没有用旧源码覆盖 fork 的 `master`，没有强推。上游 examples 和独立 integration_tests 模块予以保留；它们之前没有随 Go module archive 嵌入产品，不代表曾有意删除这些上游测试。

初始历史重放完成时，所有嵌入文件一致。随后仅新增 CI、大 Key 边界测试、维护说明，并修正已有清理测试的启动顺序；客户端运行时代码及依赖版本不变。远端下载后，对原 175 个文件中的 173 个逐一 `cmp` 通过，另外两个差异是 `KUBEBRAIN_PATCH.md` 和 `tikv/kv_test.go`，均已说明。

只有在远端 branch/SHA 核验、模块下载和 `go mod verify` 通过后，才移除产品的 175 个内置文件。旧副本可从上述产品 commit 或独立客户端历史恢复。Dockerfile、开发探针和产品 CI 同步去除本地目录依赖；探针镜像输入摘要继续包含 go.mod/go.sum，因此客户端版本变更会改变探针缓存键。

## 测试职责与已知结果

| 仓库 | 验证职责 |
| --- | --- |
| 客户端 | build、vet、全量单测、race、大 Key 边界、Region/副本路由、超时和清理；独立漏洞扫描 |
| KubeBrain | 固定远端依赖及无内置副本的回归检查、产品 build/单测、生产测试四分片、etcd 兼容与真实 TiKV 集成测试 |

客户端 [CI run 34251930827](https://github.com/fivetime/tikv-client-go/actions/runs/34251930827) 使用 self-hosted Runner，终态为 failure：test job 成功、security job 失败。旧副本和迁移后普通单测通过；迁移后本机 build/vet、全量单测与 race 均通过，远端 build/vet、单测和 race 步骤也已通过。

产品在移除内置目录后，`go build ./...`、`go vet ./...`、除生产门禁包之外的所有根模块测试通过；提交前后均执行生产门禁 `--verify 4` 及四分片，两轮各 703 项全部通过。`go test -race ./pkg/storage/tikv ./cmd/option`、build/workflow 回归以及 etcd-client-compat 的开发探针回归均通过。

真实 TiKV 验证在用户授权的 `tk-001-003` / `kubebrain-dbaas-test` 执行，PD cluster ID `7683177044639569228`，后端 v8.5.3、使用 rook-ceph 消费者卷。用当前远端依赖编译的 `TestLargeKeyRoundTripTiKV` 在 worker3 的专属临时 Pod 执行，完成 600 KiB 和 2 MiB 物理 Key 的 Put/Get/Iter 字节级 round-trip，1.95 秒 PASS，Pod Succeeded/exit 0；用例清理自己的测试前缀。没有执行 skip，也没有使用分仓前镜像冒充新客户端。

测试二进制 SHA-256 为 `a1af25f6b947367e974806ecc5a465e45504ab3fca9775e8f19684a0e7491058`，Pod UID `0291d0d9-6b06-4de1-a035-f346c433577f`。日志和终态 JSON 保存在仓库外 `/root/.local/state/kubebrain/tk-001-003/client-split-large-key.log` 与 `client-split-large-key-result.json`；临时 Pod 按 UID 删除，本机临时测试二进制已清理。

原 `TestKV/TestRURuntimeStatsCleanUp` 在 race 模式 10 次复测中出现 6 次失败：测试在客户端启动后才启用仅启动时读取的 failpoint，使后台清理可能保留正常 30 分钟间隔。仅修正测试，改为启用开关后创建测试客户端，并有界等待相同的“map 必须清空”断言；race 连续 50 次通过。不改生产清理逻辑，不跳过断言。

2026-09-08 的客户端独立漏洞扫描未通过，报告 6 项可达问题：Go 1.26.5 的 GO-2026-6091/6090/6089/5972、grpc v1.54.0 的 GO-2026-6061、x/net v0.8.0 的 GO-2024-2687。独立 security job 保留非零失败，不使用 continue-on-error。KubeBrain 自身选择 grpc v1.83.0、x/net v0.57.0，不能将客户端独立依赖图的结果直接套到产品；产品编译器和实际依赖图仍需分别扫描。这些安全升级不混入本次无运行时变化的分仓，当前不能称为安全验收通过。

## 分仓后的安全升级

当前固定客户端为 `v2.0.8-0.20260908172918-b5b63af11282`，commit
`b5b63af1128266a51c1ee63a8abbcf6f23ef5a6b`；前述 c29 版本及 RED 扫描保留为迁移历史。
新模块校验和 `h1:VNhN3wjFEMGhJ2CBwKV3vehCWkEDBcXM/pqBiL4/MSk=`，go.mod 校验和
`h1:V4mVPYUvt3A19cV1MoZtZdy3LrQ+xdzAnSbWXEz9wCI=`，已通过远端下载及模块验证。

客户端升级 Go 1.26.8、grpc v1.83.0、x/net v0.57.0 和 etcd 客户端 v3.5.33；依赖要求
使最低 Go 版本提高到 1.25.0，不再声称兼容 Go 1.19。TiKV/PD 基线和客户端运行时补丁未变。
提交前后本机 build/vet、全量单测/race、govulncheck 均通过；远端
[CI 34257592017](https://github.com/fivetime/tikv-client-go/actions/runs/34257592017)
的 test/security 两个 job 均 success。真实 TiKV 大 key 复测 1.45 秒 PASS。
产品自己的依赖升级、检查边界及未完成项见[安全升级记录](security_baseline_20260908_cn.md)。

## 后续客户端或 TiKV 升级流程

1. 在客户端仓库 fetch 上游，选择明确的 release/commit，从维护分支创建升级分支；保留上游祖先关系，不强推已发布的依赖历史。
2. 按补丁清单逐项检查大 Key、TLS CN、取消传播、资源清理、Region/Store 替换、protected checkpoint 和扫描内存语义。没有 Git 冲突不代表这些补丁不受影响。
3. 合并选定上游版本；若上游已有等价修复，记录其 commit 和替代回归测试后再移除本地补丁。通过客户端单测/race/安全检查，核验支持的 TiKV/PD 版本。
4. 发布客户端的固定 commit/version，再在 KubeBrain 单独更新远端 replace 及 go.sum。可用 `go list -m -json github.com/fivetime/tikv-client-go/v2@<已发布的完整 SHA>` 获取 Go 生成的伪版本；不要手写时间戳或使用浮动分支。
5. 运行产品 build、回归测试和真实 TiKV 集成测试：大 Key、事务、扫描、HashKV/checkpoint、TLS、故障切换。编译检查接口，`*_test.go` 锁定已覆盖行为，真实集成测试验证服务端交互。
6. 记录两个仓库的 commit、依赖校验和、TiKV/PD 版本、CI 链接和结果，再发布产品镜像；产品升级与客户端版本变更均可独立回退到已验证提交。

日常开发如需跨仓联调，可在仓库外使用临时 Go workspace；提交与 CI 必须验证远端固定依赖，不能把本机路径写回正式 go.mod。

## 向上游贡献

上游[贡献规则](https://github.com/tikv/client-go/blob/master/CONTRIBUTING.md)欢迎修复 PR，要求 DCO 签署；最终是否接受由维护者决定。本次只完成分仓，没有向上游创建 PR 或 issue。

贡献前应在最新上游单独复现问题，检查已有 issue/PR/修复，再拆为独立的最小补丁和回归测试。通用取消、资源清理和错误重试可以优先评估；KubeBrain 专用 checkpoint 扩展应先讨论可通用化的接口。2026-09-08 核对的 upstream master 为 `08cbf831121a7944bf9dd7bdda0670a8c391caeb`，其 memdb 已重构为 ART 等实现，不能把 v2.0.7 的 27 条补丁整体直接提交或假定仍全部必要。测试通过是必要验证，不等于维护者已经认可设计、兼容性和维护成本。
