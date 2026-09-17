# 控制面测试自动回归接入

最终远端结果：`35206137761`，同源 `0ad6d7b65170232b9df763c49e78530b9f59f6c3`、
attempt 1、completed/success；watch session 90526 退出 0。控制面定向 race
10.575s，完整服务 142.246s，Auth/Lease/Watch race 分别 90.908/78.030/25.707s，
proxy/revision/log-capture race 分别 5.334/8.453/5.794s，全探针 race 353.357s。
本结果不覆盖之后的 Lease 状态锁修改。

归档 session 57101 退出 0，私有证据
`/root/.local/state/kubebrain/controlplane-ci-0ad6d7b6.rw2jS0Qz/`。
run.log SHA-256：`205334faf4e387d953646fd20f7f281df737bb11986d480b09c255607f707523`。
归档的 run.json 核验了完整源码、attempt 和成功终态，不只依赖绿色截图。

2026-09-17，基于 `59f45c3c` 补齐 dbaas 自动回归覆盖。
`probe-regression.yml` 增加 scale-lab、etcd-client-compat 和 PKI 脚本触发路径，
在既有 self-hosted 作业中执行 ControlPlane / Ephemeral PKI 全部定向 race 测试。
该步骤只执行本地夹具和拒绝准入测试，不部署 Kubernetes，不运行真实规模负载。

首次工作流契约测试 RED 明确发现缺失触发路径。初次依赖预演还发现：沿用旧 CI
删除本地 replace 后 tidy 的方式，会因 `cache/v3.7.0-beta.0` 不存在而失败。
因此新步骤从官方 etcd-io/etcd 检出固定提交
`5cd9f4ee13801e18825d661e5005ae599460bc3a`，再次检查实际 HEAD，并把五个
etcd 模块替换到该工作区 checkout；不依赖 Runner 的 `/root/etcd`，不使用浮动分支。
官方仓库 commit API 已确认该 SHA 存在。旧通用 CI 的依赖问题未在本次一并修改。

本地以相同固定提交的干净源码、私有临时 modfile 验证替换、tidy、verify 和
完整定向测试：session 41392 退出 0，race 10.347s；工作流契约完整 race
session 71111 退出 0，2.477s。仓库 go.mod/go.sum 未改动。
此前尝试直接列出测试文件编译因共享 helper 缺少符号而失败，未采用该方案。
这些是本地证据，不冒充远端 CI 成功。新增工作流需要实际远端执行后再确认。

远端步骤核验：`0ad6d7b6` 的手动回归 `35206137761` 已实际完成固定 etcd
检出及 `Verify isolated control-plane harness contracts`，两步 conclusion 均为
success。09:46 UTC 作业其余服务/探针回归仍在运行，不称整体 CI 已通过。

同轮只读残留检查 session 57364 退出 0，证据
`kwok-candidate-3e813b99.ZNyd87Zs/residual-inspection.JZfZI9qo`：
租约 `0003a0aea810bc02` 剩余 TTL 2926 秒，仍存在；两条租约无附着键，
专属前缀为空。没有 Revoke/KeepAlive，转发已回收，不宣称已自然过期。
