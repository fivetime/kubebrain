# 后端隔离时的同伴协助释放：实现前安全设计

状态：2026-09-18 草案，基于 `f0b50b9a50188913dbb0cdb7b5ff4129e03ba479`。
**完整协议尚未实现、未开放新接口、未部署。** 本文不替代原 30 秒故障验收，亦不声明
已解决故障切换。现有 30/25/0.5 秒选主参数和默认 2PC 保持不变。

## 问题与已核实的现有路径

client-go v0.36.2 按候选者本地观察到变化记录的时刻加持久记录中的租期判断
是否可接棒，不直接相信远端 RenewTime。当前持久租期为 30 秒，原验收预算
也是从故障安装前至原 RPC 响应的总计 30 秒，包含诊断、选举和网络恢复。
最坏情况下，单靠租期过期接棒没有为其他阶段留下预算。

已核查以下源码，均不足以直接实现隔离后的提前接棒：

- `pkg/server/service/leader/leader.go` 的 `EnsureVoluntaryRelease` 要求
  Campaign 已停止，随后由本节点 Get/Update 后端；旧节点被隔离时无法使用。
- client-go 的 `release` 本身也依赖 Get/Update 后端；取消本地 context
  不等于共享记录已经释放。
- `pkg/backend/election/election.go` 的 Update 使用选主记录 CAS，并在
  新任期获取领导权时原子轮换全部持久写隔离令牌。该机制保护持久写，不能
  单独证明仅内存的租约续期已停止。
- `pkg/server/etcd/lease.go` 在实际发布内存期限前再次检查 epoch/freshness。
  freshness 失效是时间状态，不是永久禁止旧任期再次续约的承诺。
- `pkg/server/server.go` 的 peer 与 client 注册共享服务，peer HTTP 还包含
  版本与状态接口；现有端口或一般客户端证书不能自动作为“允许释放领导权”的授权。

因此，本方案只研究**旧 holder 主动、不可撤回地退出指定任期后，由健康同伴
代为提交释放**。不提供任意成员强制抢占仍然有效 holder 的能力。

## 必须满足的协议边界

1. **不可逆本地退位。** 旧 holder 在生成释放凭据之前，先对精确任期设置
   不可逆准入关闭状态，取消并 join 该任期选举更新及初始化工作。迟到的
   Update 成功、初始化回调和 stampRenew 均不得重新打开该任期。仅清零
   lastRenew 不足以作为此证明。必须与 lease 期限实际发布点协调，避免
   检查已通过但尚未完成发布的旧续租越过退位确认。
2. **请求身份。** 请求绑定 DBaaS 实例/选主 keyspace、旧 holder 身份、
   共享任期、精确原始记录和持久隔离令牌。普通客户端即使拥有该实例的
   客户端证书，也不得构造有效请求。新接口只放在 peer 面，不在 info/client
   面注册，不允许 plaintext fallback。具体成员证书身份映射必须在实现
   前明确，不能仅用“证书由某 CA 签发”或源 IP 作为 holder 授权。
3. **限定作用。** 同伴只能尝试清空该请求绑定的旧记录，不能指定新 leader、
   延长租期或写入用户数据。请求和错误不得输出私钥、客户端凭据或可重放令牌。
4. **原子性与恢复隔离。** 接收方在同一事务中验证精确旧记录、旧任期令牌及
   restoration fence，再提交释放。已观察到其他 holder、其他 term、相同
   holder 的重新获取或不同 token 时均不得修改。不能使用接收方当前缓存
   的 lastVal 代替请求绑定的值，否则延迟请求可能释放后来的领导权。
5. **不确定提交。** 旧方或接收方超时不证明事务未提交；重发同一个请求只能
   得到幂等结果或过期拒绝，不能再次释放新 term。旧方从不因超时或失败撤销
   本地退位承诺。无法证明条件时回退到正常租期过期选举，不强行成功。
6. **续租连续性。** 健康节点仍须通过正常原子获取、修订初始化、租约恢复和
   readiness 门禁才能服务。证明新节点不会提前删除任何已确认 KeepAlive
   对应的键；不能用持久写 CAS 或返回 TTL=0 冒充内存续租连续性证明。
7. **原流保留。** 原过期 KeepAlive 请求仍是唯一一次客户端请求，允许其在
   旧连接上等待并沿既有转发路径完成。不得以客户端重新连接/重发、另发
   KeepAlive 或提前关闭原流来替换验收对象。

## 分阶段实现与反例测试门禁

| 边界 | 必须覆盖的反例或成功证据 |
| --- | --- |
| 本地退位状态机 | 已退位后迟到续约成功、延迟初始化完成、取消后重入均不能恢复同一任期；新任期正常获取不受影响 |
| 正在执行的内存续租 | 在 freshness 检查后、期限实际发布前暂停，退位确认不能先越过；退位确认之后不再确认旧任期续租 |
| 后端原子释放 | 相同 holder 不同 token/term、其他 holder、更新过的原始记录、恢复门关闭均拒绝；事务冲突不部分修改 |
| 不确定性与重放 | 释放已提交但响应丢失，随后新任期获得领导权，再重放旧请求不得触碰新记录；旧节点重启亦不得让凭据重新生效 |
| 鉴权与隔离 | 未认证、普通客户端证书、错误成员、其他实例、plaintext、过大/畸形请求全部拒绝且无后端修改 |
| 连续性与公开协议 | 已确认续租在新任期保持有效；原过期请求继续等待真实撤销或转发，返回真实响应和正确 term |
| 真实网络故障 | 对同一 Pod UID/进程观测旧等待退出、新非零 leader/更高 term、恢复后的原 RPC 响应，三项独立留证 |

优先完成并验证本地不可逆退位与后端精确条件释放两个内部边界，再接入认证
传输；不能先开放控制接口再补授权。旧镜像滚动混用时，缺少能力的成员应
继续正常租期选举，不能导致对方超时后不安全接棒。

## 验收仍然开放

即使该协议实现完成，25 秒本地自我隔离加接棒初始化、网络恢复等也不自动
证明总计 30 秒可达。必须保存故障开始和恢复开始时间，以及每次 Status
请求的独立结果、起止时间和错误；诊断不得重置原全局时限。
不能复用已结束实验的执行标记或覆盖其失败证据。超时结果仍是失败，恢复
成功只证明环境恢复，不改变测量结论。

## 第一项内部边界：每轮选举的 freshness 不可恢复

本地工作区增加 `renewalTerm`：每个 `elector.Run` 独享一个状态，成功续约
通知与退位通过同一互斥锁串行处理。释放前标记该轮已退位并清除 freshness；
即使 release 在 Get 阶段失败、未进入 Update，也在 Run 返回后退休该状态。
Campaign 在上一轮生命周期 join 后才创建下一轮状态。旧轮次后续的 renew
或重复 retire 均不再操作新轮次的时间戳。

这不是远程释放授权，也不是事务撤销：模拟存储已提交但成功通知延迟时，
本地退位后不再恢复 freshness，但共享记录仍可以属于旧 holder。
未加入网络端点、凭据或新持久记录格式。

验证：完整 leader 包 race 重复五次通过（8.284 秒）；server 与后端 election
包 race 通过（2.205 秒、1.130 秒）；leader vet 和 diff 检查通过。移除
`renewalTerm.renew` 的已退位检查后，两个确定性负向测试在预期 freshness
断言失败；该校验只证明新状态机的必要性，不宣称原顺序 client-go 流程已经
在生产发生此并发。额外并发用例确保退位与续约通知竞争后状态仍为失效。

仍需实现内存期限发布与退位确认的协调，以及精确 token/记录 CAS、身份认证
和防重放，才能形成完整协议。目前不得向同伴发出“已安全退出”的凭据。
上述工作区变化尚未经对应提交 CI、镜像或真实集群验证。

## 复用现有租约排空屏障

进一步源码检查发现 `StopLeases` 已先获取 `leaseCheckpointMu` 独占锁，再在
`leaseMu` 下清空旧 generation。普通、授权和无 checkpoint 快速续租均通过
共享 checkpoint 屏障进入；过期等待在等待期间已释放它。因此应复用该屏障，
不再新增一套锁。`OnStoppedLeading` 现在先不可逆退休该轮 freshness 通知，
再执行原有生命周期清理；Run 返回处保留幂等退休作为补充。

新增 `TestLeaseRetirementDrainsDeadlinePublication`，在普通及无 checkpoint
快速续租的最后 freshness 检查处暂停，保留已经通过的旧检查结果，然后
发起退位与 StopLeases。测试通过真实共享锁准入被独占等待者阻止来确认排空
正在等待，解除暂停后才允许清空旧状态，并验证后续旧任期续租被拒绝。
两个路径各十轮 race 通过（1.825 秒），etcd 包 vet 通过。

这是内存修改的排空保证，不是“此前形成的响应已全部送达”保证；响应可以
晚于本地线性化点到达客户端。完整协议仍须证明恢复后的期限覆盖这些已确认
续租，且必须先关闭/停止旧选举更新、等待该轮生命周期 join，再授权远端
精确条件释放。尚未实现授权、凭据、重放防护或真实 30 秒验收。

追加授权路径验证：第三个场景先启用鉴权、建立非 root 用户及可写范围，创建
实际绑定租约的键，再进入授权续租的独占 binding 锁路径。与普通、快速路径
一起各十轮 race 通过（2.670 秒），etcd/leader vet 通过。测试先 join 续租与
清理协程，再断言结果。完整 etcd 普通回归随后通过（147.390 秒），扩展
`Lease|Revoke|Expiry|Checkpoint|Attachment` race 回归通过（81.772 秒），
顺序执行进程终态为 0，被测产品及测试源码摘要一致。证据位于
`/root/.local/state/kubebrain/renewal-term-retirement.h9XnbICi/full-regression/`。
这仍是本地回归，不是当前工作区的发布 CI 或实际集群验收。

## 第二项内部边界：精确条件释放事务

新增未导出的 `resourceLock.releaseRetiredOwnership`，尚无服务调用入口。
传入的是内部事务前提，不是认证凭据：包含旧 holder、精确原始记录和 UUID
任期令牌。先验证记录形状、holder 一致性和有效任期/租期，再在一个批次中：

- 对精确原始选主记录执行 CAS，清空 holder，保留 transitions；
- 对恢复控制键及所有恢复分片执行 Open→Open CAS；
- 对全部写隔离分片执行旧令牌→新随机令牌 CAS，立即阻止旧持久写继续提交。

任一比较失败均不得部分释放或部分轮换。实现不读取或更新接收者的选主缓存，
不会用接收者后来观察到的 lastVal 替换请求条件。重复请求返回原条件不满足，
不是伪造“本次释放成功”；提交错误也不能让旧 holder 撤回本地退位承诺。

内存存储回归覆盖：已续约记录、错误令牌、任一分片变更、恢复控制/分片关闭、
错误 holder、畸形记录、已取消 context、同 holder 再次获取后的旧请求重放；
逐分片检查旧写令牌已失效。故意模拟“事务提交后丢失响应”后，再获取新任期
并重放旧请求，确认不修改新记录。释放与新任期写入并发时恰有一方成功，所有
分片属于同一获胜事务。该竞争测试验证原子性，不授权候选者提前抢占有效租期。

完整 election 包 race 重复十次通过（5.920 秒），vet 与 diff 检查通过。
隔离 overlay 将令牌 CAS 改为无条件 Put 后，错误令牌和变更分片用例均在预期
断言失败。原源码未受该负向变体影响。此前完整 etcd 回归发生在添加本原语
之前，不冒充包含此新增文件的全量结果。

仍未实现对端身份认证、本地退休证明的生成及传输、能力协商，也未运行真实
TiKV 竞争/分区验收。不得将内部结构体直接反序列化为已授权网络请求。原 30 秒
故障门限仍未通过；所有本轮改动均未部署。

## 条件快照的本地来源

增加未导出的 `ownershipSnapshot`，在 resource lock 的同一互斥临界区内复制
当前本地 holder、精确原始记录及已安装令牌，再检查字段/任期一致性。
不发起存储读取或续约；返回字节与缓存分离。它是事务前提快照，**不是退位
证明或当前所有权授权**。缓存可能落后于实际集群，仍须由接收方的原子 CAS
重新验证，不能凭它提前接棒。

续约 Update 的事务已提交但后续 TSO 读取失败时，既有逻辑已保存提交后的
CAS 值，快照可保留这一条件；首次 Create 提交后失去确认而尚未安装本地
令牌时，则返回无快照，不猜测令牌，也不读取后端重新构造授权。后者需要
回退到正常选举。已撤回、外部 holder、raw/cache 任期不一致、畸形记录、
缺失或无效令牌均不输出可用快照。

测试用不可访问后端验证无 I/O，用修改返回字节验证无缓存别名，并验证上述
两种提交后 TSO 失败边界。最终 election 包完整 race 重复十次通过
（7.098 秒），vet/diff 检查通过。快照和条件释放仍无服务调用入口；完整
认证、不可逆退位证明、传输及真实集群验收继续开放。

## 选举生命周期的退位完成边界

leaderElection 增加内部可选的 `onTermRetired` 回调，仅对真正取得过共享锁的
选举轮次执行，位置在 `elector.Run` 返回、不可逆 freshness 退休和轮次
context 取消之后，且同步执行在下一轮竞选之前。现有 OnStoppedLeading
已等待清理回调及 OnStartedLeading 协程退出，故这里可以作为后续提取条件
快照的位置。该回调尚未接到服务构造或网络发送，不是已经生成的认证凭据。

回调收到外层 Campaign context：正常局部失败不取消整个 Campaign；进程
停止时该 context 可能已取消，回调不能自行忽略取消无限延长停机。后续网络
实现还必须设置操作预算，失败时保持已退位状态而不是重新打开旧轮次。

测试用实际 client-go Campaign 分别阻塞清理和初始化退出，确认不会提前
通知；未获取共享锁的失败竞选不产生通知；首次初始化失败时阻塞退位回调，
确认下一轮尚未启动，释放后正常重新获取。最终 leader 与 server 包完整
race 各五轮通过（8.798 秒、6.287 秒），vet/diff 检查通过。此前全量 etcd
回归不是本次新增回调后的结果，不作混用。

## 持续回归入口

`probe-regression.yml` 的 leadership 步骤现包含 `pkg/backend/election`
全包的 vet 和非缓存 race 测试（单包三分钟上限），不再只检查 leader
服务层。既有 `pkg/**` 触发范围覆盖条件释放与快照文件。工作流约束测试
先在缺少命令时失败，再在补齐命令后通过，防止后续无意移除该覆盖。

本地执行 `go test -race -count=1 -timeout=3m ./build ./pkg/backend/election
./pkg/server/service/leader` 全部通过，分别为 2.578、1.783、2.727 秒。
这只证明当前本地工作树的这些包；尚未提交或触发远端 CI，也没有新增
真实 TiKV 原子性、认证退位协议或原 30 秒门限通过证据。

## 真实 TiKV 条件释放测试（2026-09-18）

新增 `TestRealTiKVRetiredRelease`，通过既有
`hack/backend-integration/run-real-local.sh` 的一次性 Docker PD/TiKV 环境执行。
不开放私有方法、不接受脚本外部数据库地址、不操作专用测试集群。测试连接前
限制随机前缀格式与预期 cluster ID，写入前核验实际 cluster ID 和空前缀，
清理仅删除本次精确键集合并在同一事务比较 owner 标记。

用例验证错误令牌、关闭的 restoration shard 均不产生部分释放；随后让一个
真实旧事务完成 ownership CAS 读取和数据写入暂存，在 Atomic 回调处暂停，
由另一个事务完成退位及全部令牌轮换，再恢复旧事务。旧事务必须返回
`ErrCASFailed`，payload 不得可见。最后同名 holder 重新取得领导权，旧释放
请求重放必须失败且不改变新记录与令牌。它不是只在 BeginBatchWrite 后暂停，
也不把任意超时错误当作隔离成功。

普通模式整套入口通过（session 35611），新增用例 2.58 秒；该首次执行使用
较宽的旧事务失败断言，随后已收紧为上述明确冲突断言。收紧后的整套 race
入口通过（session 1928），新增用例 5.71 秒，证据
`/tmp/kubebrain-real-protocol.UmcQUj8QHu`。源码摘要在执行后核验一致，退出码
0、cleanup_failed=0；额外按本轮 owner 查询确认容器与网络均无残留，两份
测试二进制也不存在。普通模式证据 `/tmp/kubebrain-real-protocol.QJHeMoba11`，执行后
源码摘要核验一致；该结果不冒充后续收紧版本的执行结果。

入口新增独立 election 测试二进制，仍要求命名用例明确 PASS，不允许 SKIP
充当成功。正常退出及中断均清理两份二进制。真实 Docker 中断检查在 PD、TiKV
创建后分别发送终止信号，两次均为预期退出 143，精确 owner 的容器、网络及
二进制全部不存在；总检查退出 0，证据
`/tmp/kubebrain-protocol-interruption.KlhkjPlQbr`。本地 build/election 全包 race
通过（2.593/1.802 秒），vet 和 diff 检查通过。

上述测试只验证内部存储事务，尚不证明认证退位协议、跨节点租约连续性、
多节点存储故障恢复或原 30 秒验收门限。当前改动仍未提交、未部署、未触发
远端 CI。

## 退位清理必须晚于初始化退出（2026-09-18）

继续审查发现原生命周期顺序是：撤销 leader、清理服务状态，再等待
OnStartedLeading 退出。初始化可能在取消后完成先前操作并发布 leader 或
readiness，使清理被晚到的发布覆盖。仅证明退位通知等到了两个回调都返回，
不足以证明通知发生时状态已经清理干净。

现在先不可逆撤销 freshness 并撤销 leader，再等待本轮初始化退出，随后再次
撤销 leader 并执行最终服务清理，最后才允许退位通知或下一轮竞选。停止回调
只执行一次。revision 初始化返回后检查取消；serving 阶段在开始、返回和最终
发布 readiness 前也检查取消。这些检查不能消除检查后的取消竞争，所以仍需
最终清理位于 join 之后。初始化不响应取消会阻塞 join，不能因此提前出具退位
完成证明；这不是有界网络退位协议的完成声明。

新增回归在 revision 与 serving 两个阶段分别阻塞到取消后，再允许成功返回，
验证清理不提前发生，退位完成后 leader/readiness 不会复活。修改前两个子用例
均失败（session 3613）。另一个测试覆盖进入阶段前已取消、执行中取消后返回
成功两种情况，修改前也均失败（85057）。最终 leader/server 完整 race 五轮
通过（78173，9.145/6.233 秒），vet/diff 检查通过。CI leadership 步骤同时加入
server 包独立 race/vet 及工作流约束，覆盖实际服务初始化回归。

本节变更晚于上一节 TiKV 事务测试，不能把其存储层结果当成本次服务层整套
真实集群验收。认证退位、原任期快照绑定、租约连续性和原 30 秒门限仍开放。

## 固定 etcd 源码对照与后续认证约束

再次核对 `/root/etcd` 的固定提交
`5cd9f4ee13801e18825d661e5005ae599460bc3a`：

- `server/etcdserver/server.go` 的 `applyEntryNormal` 在本节点仍为 leader，
  且前一任期条目应用完成后的空条目路径调用 `lessor.Promote(ElectionTimeout)`。
- `server/lease/lessor.go` 的 Promote/Demote 都受 lessor 互斥锁保护；Demote
  将租约到期时间设为无限，清空 checkpoint 与过期通知调度，并关闭 demotec。
- KubeBrain 并无相同的 Raft 应用队列，不能把 client-go 的获取成功视为上述
  状态恢复边界。必须保留 revision 初始化、租约恢复和 readiness 门禁；本地
  退位须先禁止旧轮次写入，再 join 初始化，最后清理该轮次发布的状态。

现有 `pkg/endpoint/config.go` 的 `appendPeerIdentityVerification` 只按
允许的 CN/hostname 接受连接；它不是证书到特定 holder、实例的授权映射。
`--peer-client-cert-file` 可提供独立的出站证书，但允许连接仍不等于允许
代为释放任意成员。后续接口必须另外绑定已验证的对端身份与请求中的 holder
及实例，并拒绝公共客户端证书、共享身份下无法区分的成员声明和明文连接。
在映射明确且反例测试通过前，不能将内部快照直接序列化成可执行控制命令。

## 当前整合版本的服务端回归（2026-09-18）

在包含最终 join/cleanup 顺序及取消检查的完整工作树上执行：

```sh
go test -count=1 -timeout=12m ./pkg/server/...
go test -race -count=1 -timeout=8m ./pkg/server/etcd -run '(Lease|Revoke|Expiry|Checkpoint|Attachment)'
go vet ./pkg/server/... ./pkg/backend/election
git diff --check
```

同一执行 session 85253 最终退出 0。服务端所有包普通回归通过，其中 etcd
包 150.321 秒；租约相关 race 回归 81.709 秒。受测源码在执行期间保持不变，
摘要复核通过。日志与摘要保存在
`/root/.local/state/kubebrain/retirement-full-regression.QI9qLtdr`。

本次将上述实现、测试与记录整合为本地提交。前文“尚未提交”保留各阶段当时
状态，不代表该整合提交后的状态。仍没有本提交的远端 CI、候选镜像或真实
集群故障验收结果；更广的生产就绪目标以及认证、连续性差距均未关闭。

## 独立的 holder 身份授权组件（2026-09-18）

`pkg/server/peer_retirement_auth.go` 增加内部授权组件，尚未注册到任何产品
transport，也未新增可启用的 CLI 配置。它要求接收实例有显式的
`holder -> SHA-256(SPKI)` 公钥摘要映射；映射只能来自实例管理配置，不能由
请求提供。一个 holder 可有多个密钥以支持轮换，同一密钥不能授权多个
holder；共享 peer 密钥的部署不能直接启用这一方案。跨实例也应使用独立
peer 密钥，不能把同一密钥配置到隔离实例后仍宣称具备密钥级隔离。

检查只接受 transport 提供的完成握手、TLS 1.2 以上状态，要求已验证证书链
绑定精确的对端叶证书，并以服务端当前时间复核证书链有效期，再比较实例、
holder 与公钥摘要。不采信 CN、源 IP 或请求提供的证书和时间。TLS 的 CA
验证仍是前提，摘要白名单不是绕过证书链验证的替代品；nil 组件拒绝授权。

测试使用真实生成的同 CA、同 CN、不同密钥证书完成 mTLS 握手：配置 holder
及其轮换密钥通过，普通客户端密钥、其他 holder 的密钥、其他实例均拒绝。
另外覆盖无验证链、叶证书不匹配、过期/尚未生效、明文、未完成握手、旧 TLS
版本和配置共享密钥。五轮 race 用例通过（83283，1.775 秒）。Mutation overlay
仅移除公钥授权检查后，三个真实 TLS 越权用例按预期从 403 变成 204 并失败
（46323），证明它们不只是在验证 TLS 握手是否成功。原源码未被替换。

该组件只证明发送方是被配置授权的 holder，不证明其选举/租约已经退位。
后续仍须把精确条件快照绑定到已 join 的生命周期，再接入有预算、限流、
大小限制和能力回退的 peer 传输；普通 client/info 面不得注册释放接口。

最终 server、endpoint、election、build 全包 race 均通过（57918，分别
2.409、15.663、1.907、2.571 秒），server/endpoint vet 与 diff 检查通过。
本节随授权组件做本地提交；未推送、未触发 CI、未修改测试集群证书或配置。

## 将条件快照绑定到具体选举轮次（2026-09-18）

新增不透明、无可写字段的 `election.OwnershipCondition` 及可选 provider。
provider 只在本地缓存与调用方指定的精确已确认记录一致时返回分离的条件，
不读取后端、不开放释放方法；普通格式化输出隐藏条件内容。它仍不是授权或
退位凭据，不能因为类型名而跳过认证、生命周期和 CAS 校验。

每次成功的 own-holder Create/Update 返回后，选举包装层将对应记录交给本轮
`renewalTerm` 捕获。退位标记与捕获使用同一把锁，退位后迟到的成功通知不再
替换条件。Campaign 在本轮初始化 join 和最终清理之后、下一轮开始之前，
把冻结条件及 available 标记传给内部退位回调；不在此时查询共享缓存。
回调仍未通过服务构造暴露，也未接入网络传输。

缺少 provider、无法匹配精确记录时返回不可用，后续必须回退正常选举。
已提交但未确认的更新可能使上一次快照过期，接收方精确 CAS 将拒绝它；不能
为了加速接棒从缓存猜测新记录。正常释放成功后冻结条件也可能已经过期，
这不是再次释放新任期的许可。

测试覆盖 live 轮次不输出退位条件、退位后跨任期缓存变化和迟到通知不能
替换快照、下一轮可捕获自己的新条件、不支持 provider 的回退。实际 client-go
Campaign 配合真实 resourceLock/memkv，确认共享缓存已经正常释放为空 holder
时，post-cleanup 回调仍收到先前捕获的条件。backend 测试要求精确写入记录匹配，
并验证快照不随下一次获取发生别名变化。

完整 election、leader、server race 五轮通过（96407，4.535/9.425/7.111 秒），
三个包 vet 及 diff 检查通过。移除捕获路径的 retired 检查后，跨任期快照测试
按预期失败（29871）；只使用 overlay，原源码未被替换。该证据不代替真实
网络传输、跨节点租约连续性或原 30 秒验收。改动做本地提交，未推送或部署。

## 条件消息编码草稿：不等于传输授权（2026-09-18）

在 `d5b20ef6` 的 CI 运行期间，本地增加独立编码草稿，尚未提交或推送。
`OwnershipCondition.MarshalBinary` 编码版本 1、holder、精确原记录字节与
令牌，记录通过 base64 编码，避免 JSON 再序列化改变 CAS 条件。输出含可重放
条件，不得记录到日志。`ParseOwnershipCondition` 必须接收 transport 已为本
实例验证的 holder，而不是把请求自报的 holder 当作认证结果。

编码消息上限为 96 KiB，原始记录仍限制 64 KiB；transport 以后必须在读取时
就限流和限长，不能只依赖这里对已缓冲数据的检查。解析拒绝未知版本、未知
字段、重复字段（含转义后的同名键）、缺失字段、尾随 JSON、空凭据、无效令牌
及内外 holder 不一致。解析不读取或修改后端；条件释放继续是内部方法。

测试验证往返及原始字节保留、输入缓冲区无别名、解码后释放一次成功及重放
冲突、大小精确边界、畸形和跨 holder 拒绝。election/leader/server 完整 race
五轮通过（37838，4.768/9.596/6.871 秒），election vet 与 diff 检查通过。
两 worker、10 秒预算的模糊测试执行 10579 次，通过（41274，11.114 秒），
这不是穷尽验证。上述为本地草稿证据，不能归入 `d5b20ef6` 的远端 CI 结果。

后续格式审查发现，直接解码到 Go `[]byte` 还会接受 JSON 数字数组。新增
record/token 两个反例在修改前均失败（18880）；现在先解码字符串再做严格
base64 解码，拒绝替代数组表示。最终 election/leader/server race 五轮通过
（98224，4.993/9.292/6.788 秒），vet/diff 通过；更新后的模糊测试执行
19473 次，通过（83402，11.119 秒）。草稿仍未提交或推送。

## 已提交源码的后端 CI 结果

`d5b20ef65d39117b6f743424bde3ea3a7f64f622` 的
[后端集成 CI](https://github.com/fivetime/kubebrain/actions/runs/35313121787)
attempt 1 已完成并成功。完整日志确认 `TestRealTiKVRetiredRelease` 在普通、
race 两轮均明确通过，两个套件最终标记通过，PD/TiKV 创建后中断清理检查
也均通过。此结果不覆盖上文未提交的编码草稿，不等于镜像独立核验或真实
集群验收。镜像和服务回归 CI 在本次记录时仍运行中。

随后同提交的[服务回归 CI](https://github.com/fivetime/kubebrain/actions/runs/35313121723)
attempt 1 成功。完整日志明确包含轮次条件捕获、初始化清理顺序、真实 TLS
holder 授权及普通/race 租约发布屏障测试 PASS；镜像 CI 仍运行中。

## 接收端读取边界草稿

内部 `readPeerRetirementCondition` 将身份授权与条件解析连接起来，但未注册
任何 endpoint，也没有后端释放调用。只接受 POST、唯一且有长度限制的实例/
holder 标头，先验证 transport TLS 身份，再检查 Content-Type、拒绝压缩消息，
最后读取消息体。未知长度消息也最多读取 96 KiB 加一个检测字节；授权后的
holder 再传给条件解码器，不能改信消息内自报的 holder。调用者负责关闭 Body。

这只限制读取字节量，不保证读取耗时；真正 handler 仍须设置 transport 读取
deadline、并发/速率限制及后端操作预算。单纯 context deadline 不能中断任意
Body reader，不能将该内部函数称为可直接开放的有界网络服务。

测试覆盖未授权零字节读取、重复身份、错误方法、压缩消息、已知/未知长度超限、
精确上限、取消和消息 holder 不匹配，并确认解析不修改选举记录。server/
election/leader 完整 race 五轮通过（11512，7.101/4.991/9.511 秒），vet/diff
通过。移除 LimitReader 的 overlay 反例按预期失败（81742）：超限请求实际读取
196608 字节，超过允许的 98305 字节；原源码未被替换。这些仍是未提交草稿的
本地证据，不属于上述远端 CI 结果。

编码与读取边界在上述验证后整合为本地提交；前文“未提交”描述各阶段当时
状态。暂不推送该整合提交，以免取消仍在构建的 `d5b20ef6` 镜像任务。
本地提交不改变功能状态：仍无网络接口注册、发送端接入、实际远程释放或
跨节点连续性验收，不能声称完整协议已经实现。

本地整合提交 `14e635b3a9aa4aa5ca68db2bcdc454181074336d` 随后完成更广的
`go test -race -count=1 -timeout=5m ./pkg/backend/... ./pkg/server ./pkg/endpoint ./build`
回归，所有包通过（96918，后端主包 91.990 秒、endpoint 15.716 秒），对应
backend/server/endpoint vet 与 diff 检查也通过。该提交仍未推送；远端 CI
验证对象继续是 `d5b20ef6`，不能混用两者的验证范围。

## 固定 d5b20ef6 制品审计完成

上述三条 CI 均已成功，镜像任务为
[35313121706](https://github.com/fivetime/kubebrain/actions/runs/35313121706)，
均为固定源码 attempt 1。独立审计使用该提交的归档，而不是后续本地工作树；
检查版本、OCI 源码标签、Go 版本、TiKV fork/grpc 依赖与非 root 身份。

- index：`sha256:53b0e2d2a28bd27ed8517f17cb4928ff0426e1082f342b31b7156c3f76643601`
- amd64：`sha256:451e3c70a6a20e3d1e7effc6eb653225865786524af2b745ac912bdf2e302191`
- arm64：`sha256:5c1361985df507285995ee40cddf678d2fd4c35e05d60249516831040da7ac32`

只实际执行 amd64 的 version 命令，arm64 仅检查索引身份。证据位于私有
`release-d5b20ef6.Jgg0uUsV/image-evidence.TU3liS9I/verified.json`；审计进程句柄
已不存在，原终端输出丢失，复核了成功收据的生成顺序、清理日志以及所属容器/
提取二进制确已不存在。没有重复审计、重启 CI 或部署；这不覆盖本地 14e635b3
及后续 handler，也不代表原 30 秒验收通过。

## 有界接收 handler：尚未注册或连接后端

内部 `peerRetirementHandler` 组合既有授权、消息解析与注入的释放回调。设置
socket/stream 读取和响应写入截止时间，不支持 deadline 的包装器拒绝请求；
参照 [Go ResponseController 文档](https://pkg.go.dev/net/http#ResponseController.SetReadDeadline)
并核对本机标准库实现。HTTP/1 关闭本次连接，避免拒绝后继续排空慢请求体。
身份检查发生在消息读取和速率额度消耗之前；认证后的请求另受固定容量并发槽
及令牌桶速率限制，不排队等待空槽。

后端回调带独立 context 预算，同步执行并在返回后再次检查取消；错误或超时
只返回无正文 503，不能反射令牌或存储错误，也不声称事务未提交。回调若不
响应取消，继续占用原并发槽，不能靠启动后台 goroutine 假装请求已被终止。
因此仍须验证真实后端遵守预算。TLS 握手、HTTP 头读取及连接总数的限制属于
实际 listener 接线要求，不能把这个 handler 说成已经限制了整个服务器。

当前仍无 endpoint 注册、发送端或实际远程释放。后续构造必须把 auth 的实例
身份与精确后端 keyspace 绑定，不能接受请求指定存储地址；也仍须完成租约
连续性、版本回退和原 30 秒真实故障验收。

验证：真实 mTLS 的 HTTP/1.1 与 HTTP/2 正常请求、同 CA 未授权 key、畸形条件、
后端错误、取消后返回 nil 均符合预期；两个协议的未完成请求体由服务端读取
deadline 截断。并发槽、速率额度、未授权不消耗额度和隐藏 deadline 支持的
拒绝路径均覆盖。取消后仍阻塞的回调保留并发槽，最终返回 nil 仍得到 503。
临时移除读取 deadline 后，两项真实慢请求测试均如期失败（37724）；恢复后
最终 server/election/leader race 五轮通过（60161，9.478/4.818/9.291 秒），
三个包 vet 与 diff 检查通过。首次编译曾因比较含切片的条件结构失败，已改为
比较编码结果；不将失败阶段计作通过。本轮只有本地代码测试，没有集群变更。

## 有界发送端：真实 TLS 联调，尚未连接 Campaign

内部 `peerRetirementSender` 只接收运维配置的 HTTPS peer 地址，使用固定
`/internal/retirement/v1` 路径；拒绝用户信息、查询、fragment、自定义路径、重复
地址及超过 16 个目标。消息 holder 必须匹配本地配置。一个总 context 预算
覆盖所有尝试，每个显式目标至多一次；首个慢目标可耗尽整个预算，此时回退
正常选举，不给其他目标重置时限。没有环境代理、重定向跟随或 HTTP/1 连接复用。

TLS 要求显式 CA 与单个客户端证书，拒绝跳过验证、动态证书/验证回调以及
ServerName 覆盖；根据目标主机名正常验证服务端。最低 TLS 1.2，并保留调用者
更严格的最低/最高版本约束。CA 和证书字节独立复制，私钥对象仍要求调用者
保持不可变。响应头限制 8 KiB，不读取响应正文，只将明确 204 视为确认；旧
版本 404、200/202、冲突、超时及丢失响应统一为“释放未确认”，不输出消息体、
原始网络错误或目标 URL。这些结果都不能撤销旧任期退位。

真实 mTLS HTTP/1.1、HTTP/2 发送端到上一节 handler 的联调通过；回调收到精确
原条件。另覆盖同 CA 不同 key 拒绝、未信任服务端、错误 holder、本地取消、
重定向不泄漏请求、显式第二目标回退、总预算耗尽后不访问第二目标。模拟
回调后断开连接丢失确认，发送端报告未确认且同一目标只有一次请求；该测试
不是持久事务提交证明。最终 server/election/leader race 五轮通过（76902，
11.226/4.831/9.290 秒），三个包 vet 和 diff 检查通过。

仍未连接 post-join 回调，未在 peer listener 注册路由，联调使用注入回调而非
真实后端释放。下一步必须完成实例/keyspace 绑定、精确条件释放的跨包接线，
并证明先前已确认续租的连续性，之后才能开启专用集群实验。没有部署或改变
30/25 秒选主参数，也未将这些测试计入原 30 秒故障门限。

## 真实条件释放接线与存储作用域

在已推送 `7d8b1724` 之后，本地增加可选 `RetiredOwnershipReleaser` 能力及
`newStoragePeerRetirementHandler`。scope 为版本化的真实底层 ClusterID、
keyspace 和选主前缀元组的 SHA-256 摘要；命名空间按原始字节编码，避免无效
UTF-8 被 JSON 替换后产生别名。holder 不参与 scope，使同一实例成员一致。
backend 将已验证的 keyspace 传入锁构造；不改现有选主 key，不迁移记录。

没有 ClusterIdentifier、ID 为零或前缀为空时 scope 为空，拒绝绑定接口。
不会把 memkv/badger 面向公开 etcd 协议的合成 ClusterID 当作真实集群身份。
接收端构造要求认证策略的 instance 等于本地锁 scope；每次释放也重新检查
scope，再执行原有精确记录、令牌、恢复屏障的原子事务。请求无法指定后端
地址或 key。这个进程内能力不是认证或退位证明，调用者仍必须遵守身份校验
和不可撤回的 post-join 退位约束；尚未在 listener 注册任何路由。

真实 mTLS 到 memkv 事务联调确认：同 CA 未授权 key 无法释放，正常请求清空
旧记录并轮换全部隔离令牌。另在事务成功后主动丢弃网络确认，发送端报告
未确认；随后模拟同 holder 的新一次获取，再重放旧请求，记录及全部令牌
不变。这里的 memkv 通过测试包装显式提供 ClusterIdentifier，不能误称普通
memkv 已获得真实集群身份。构造负例覆盖不同 PD 身份、keyspace、前缀及无身份。

单独五轮 race 覆盖 backend 配置传递以及 server/election/leader（64546，
1.528/12.527/5.536/9.171 秒），vet 通过。最初 backend 测试的 Close 调用因
接口未暴露该方法而编译失败，改为现有测试惯用的具体类型清理后通过。
随后完整 backend 树、server、endpoint、build race 回归通过（92601，主后端
91.081 秒、server 3.946 秒、endpoint 15.685 秒），相关 vet/diff 检查通过。

再次核查固定 `/root/etcd` 提交 `5cd9f4ee13801e18825d661e5005ae599460bc3a`
的 `server/lease/lessor.go`：Promote 刷新期限、Demote 停止权威过期处理。当前
KubeBrain 仍在恢复时使用持久 checkpoint TTL 与选主窗口扩展，在退位清理时
StopLeases；这些代码观察不代替跨节点已确认续租的连续性证明。该验证和
Campaign/peer-only listener 接线仍未完成。没有集群部署或原门限通过结论。

真实本机 TiKV race fixture 随后终态成功（15967，`result=0 cleanup_failed=0`）。
`TestRealTiKVRetiredRelease` 已通过新的 scope 适配器执行，明确 PASS 6.47 秒，
涵盖错误 scope 无修改、旧事务已暂存后的冲突、恢复门拒绝和新任期重放拒绝。
证据 `/tmp/kubebrain-real-protocol.LJVSt2nRqp`，该用例日志 SHA-256 为
`9c79cf547ab3d2bb458fdfef3977b398c7c25186d057b33abe10b640c6b4dd04`。
独立复查本次 owner 的容器、网络及两个测试二进制均不存在，日志保留。
只运行了该套件 race 版本；这不是 TLS-to-TiKV 跨节点验收。远端 CI 仍验证
此前 `7d8b1724`，不覆盖本轮本地作用域改动。

## 构造期退位回调与隔离链路联调

新增 `NewLeaderElectionWithRetirement`，在 Campaign 启动前一次性安装
post-join 回调，不提供运行中可变 setter。原构造函数及默认行为保持不变。
回调仍同步发生在旧 elector、初始化和清理结束之后、下一次选举之前，使用
冻结条件；available=false 禁止请求，shutdown context 已取消时不发送。
发送端新增此回调的适配器，不创建后台重试队列，也没有任何失败后重新激活
旧任期的路径。构造器文档明确要求回调尊重取消并限制自身耗时。

新的跨包测试使用真实 Campaign、mTLS HTTP/2、存储作用域 handler 和 memkv
条件事务：旧 holder 已获领导权后，仅令其锁的 Get/Update 返回 unavailable，
健康同伴仍可访问同一存储。旧任期退位并 join 初始化、清理后，回调确认
IsLeader=false、freshness=false 且父 context 仍有效，再通过发送端触发
同伴提交释放。最终共享记录 holder 为空，旧后端隔离仍存在。初始化后端
由测试桩提供修订初始化，没有真实 lease service，所以不是租约连续性证明。

测试持久租期 30 秒，但本地 RenewDeadline 为 200 毫秒、RetryPeriod 为 10
毫秒，用于快速观察生命周期顺序；生产/测试集群参数完全未改。这不是原
30 秒故障门限的验收。生产 NewServer 尚未调用新构造器，listener 尚未注册
该路由，仍须完成显式配置、各 holder 独立证书及真实租约连续性验证。

远端 `7d8b1724` 的后端集成 run `35316598627` 已成功，日志核对普通/race
两次 `TestRealTiKVRetiredRelease` 及 PD/TiKV 中断清理通过。日志摘要为
`ad5f3f4af13dc4e7b1d0698276c037c9138c2e8cbe95f903ebd8cd4c16aad3d2`。
这只验证已推送的旧源码，不覆盖本地作用域和构造期接线。

本地最终 server/leader/election race 五轮通过（59769，13.760/9.618/5.512
秒），三个包 vet 与 diff 检查通过。缺少条件和 shutdown 取消的适配器路径
亦确认没有发送请求。测试未创建集群资源或编译输出文件；本轮先本地提交，
不推送取消仍在运行的旧源码镜像任务。

## 显式实验构造入口：默认关闭，peer-only

新增 `NewServerWithPeerRetirement` 和 `PeerRetirementConfig`，先完成配置
校验，再创建后台服务。普通 `NewServer` 仍不配置回调或路由；当前 CLI/部署
清单也没有启用选项。显式构造路径要求批准的 scope、至少两个独立 holder
密钥、HTTPS 同伴地址、独立发送 TLS 配置及非零资源预算。本机发送证书的
SPKI 必须属于本地 resource lock 身份，不能拿普通客户端或其他 holder 的
证书启动。策略、目标及证书字节在准备阶段独立保存。

实验路径在同一 server 构造中安装 post-join 回调，并只将固定版本路由加入
`GetPeerHttpHandlers`。client/info handler 集合均无此路由。无论路由如何
注册，handler 自身仍要求已验证的 TLS 身份。此构造 API 不创建 listener，
调用者仍须提供验证客户端证书的 peer TLS、连接准入和头部/握手时间限制；
不能把 handler 的 body deadline 误认为整个 listener 的资源防护已完成。

测试覆盖错误 scope、跨 holder/共享 key、缺少本机成员、plaintext、零预算、
单成员策略拒绝；无效配置使用只提供 GetResourceLock 的后端桩，任何进一步
构造操作都会失败，以验证校验顺序。有效配置修改原 map/URL/证书后，已准备
协议仍保持独立。实际 server 构造测试进入受控 prevalidation，确认 peer-only
路由及关闭；该 fixture 模拟已有 leader 的加入节点，并不做租约切换。

初始“完全没有选主记录、prevalidation 阻塞”的关闭测试三次失败：原有
EnsureVoluntaryRelease 等待至 5 秒后返回 deadline exceeded（34707）。本轮
没有修改这个退出策略，也没有声称为空库启动后退出已通过；将入口/路由测试
明确改为已有选主记录的加入节点后，三轮 race 通过（74200，1.354 秒）。
这个初始状态的退出可用性差距应独立复现、评估，不得被实验功能测试覆盖。

新入口仍属于明确标记的实验 API，未加入生产命令行或部署。实际租约连续性、
独立 holder 证书部署与原 30 秒故障验收仍未完成，不能作为生产就绪结论。

最终回归（74685）成功：server/leader/election race 五轮分别为
14.230/9.364/5.620 秒；endpoint/option/build race 一轮分别为
15.701/1.243/2.678 秒，相关 vet 与 diff 检查通过。没有集群变更。

## 两个独立后端的续租恢复边界测试

新增 `TestAcknowledgedLeaseSurvivesAssistedReleaseAndNewBackend`：两个独立
backend 和 RPC/lease manager 共享 memkv，仅测试包装提供 ClusterIdentifier。
旧 backend 真实获取选主记录/隔离令牌并挂接写入准入，新 backend 不共享旧
租约内存。覆盖纯内存续租及已有短 remaining-TTL checkpoint 后的续租，公共
LeaseKeepAlive 测试流明确收到成功 TTL；在 Send 边界读取持久记录，要求
checkpoint 已清零，再记录已确认的内存期限。

之后关闭旧 freshness、StopLeases，健康节点提交 scoped release，正常获取
新选主记录、初始化持久修订并 ReloadLeases。断言新恢复期限不早于旧确认期限、
TTL 为正、绑定关系和键仍存在，旧管理器不能再次续租。恢复扩展仍为 30 秒。
此处直接调用存储释放，不包含 TLS/Campaign 故障链；前几节各自的测试不能
拼接成已经完成真实网络端到端验收的结论。

检查固定参考 etcd `server/lease/lessor.go` 的 Renew 路径：持久 remaining-TTL
checkpoint 清零发生在期限刷新与返回 TTL 之前。本测试针对这一时序，而非
仅检查最终记录。首轮夹具错误使用 300 秒租约并假定 checkpointTimer 非空，
Stop 空指针 panic 后清理又等待未释放锁，最终 60 秒超时（24099）；这是新增
测试本身的错误。已改用 600 秒租约、检查定时器存在性并 defer 解锁，未修改
产品代码、原测试集群参数或原验收门限。重跑结果另记。

`7d8b1724` 服务回归 run `35316598625` 已成功（attempt 1），完整日志明确
包含 HTTP/2 慢请求、真实 H1/H2 发送接收及丢失响应不自动重发测试 PASS。
日志 SHA-256：`ea0370fa352c54f44efe85597cc7d678a8ee43414e4d192262908149d99396ff`。
镜像 run `35316598611` 仍在 Build and push 步骤，本地后续提交不能归入该 CI。

修正夹具及增加 Send 边界断言后，新增续租恢复测试与既有退位发布屏障、
checkpoint 清理、选主窗口扩展测试 race 三轮通过（42427，2.491 秒），etcd
包 vet/diff 通过。随后按 CI 的完整 `(Lease|Revoke|Expiry|Checkpoint|Attachment)`
模式运行 race 一轮，通过（82550，82.476 秒），包括新增用例。没有产品行为
修改，亦没有真实网络连续性或原 30 秒门限通过结论。

随后镜像 CI 也成功；三条 `7d8b1724` 固定源码 attempt 1、workflow 路径及
具体测试日志已独立核对成功（88847），证据为私有目录
`release-7d8b1724.7vFpQLN1/ci-evidence.sJZwBH2V/verified.json`。
独立镜像审计已开始，完成结果须另记，不能从 CI 成功推定镜像身份或部署验收。

该独立审计随后成功（22345，`PUBLISHED_IMAGE_IDENTITY_VERIFIED`、终态 0）：

- index：`sha256:09dda71afd7066f3fa9fca175f34d6bde494aad97b0b96a2f7c7f866637a2eb7`
- amd64：`sha256:432c81cbffe675affa4910af47ef8b7480c7d4c1e7c10210cfe68ed23b866a31`
- arm64：`sha256:992d6c8a146fbc406216799a247cf11c29fffe176e75b141cdad0eb7b5eb84ee`

使用固定 7d8b1724 归档检查版本/OCI 标签、非 root 身份、Go/TiKV fork/grpc
依赖；仅实际执行 amd64 version，arm64 只验证索引身份。证据为私有目录
`release-7d8b1724.7vFpQLN1/image-evidence.7tVsszDm/verified.json`，所属容器
及提取二进制独立复查已不存在。无集群部署，不覆盖后续本地四个提交。

## 启动前拒绝发送证书与签名器公钥错配

在 `8a667c24` CI 运行期间，配置审查发现只检查证书 pin 不足以拒绝错误私钥：
证书属于本机 holder、但 PrivateKey 来自另一成员时，发送器构造仍成功，直至
实际 TLS 握手才失败。新增反例先在原实现失败（9530），确认不是推测。

现在要求配置对象实现 crypto.Signer，将其 Public() 的 PKIX 编码与从证书 DER
重新解析的 SPKI 精确比较。不信任可被调用者修改的 Leaf 缓存，不把私钥或
底层错误写入日志。对畸形标准密钥在 Public() 中的 panic，仅在这个配置校验
调用内恢复并返回通用错误，避免短 Ed25519 密钥或 typed-nil RSA 导致启动崩溃。
这验证签名器声明的公钥一致性，不证明硬件签名器可用性、对端 CA 信任或实际
网络握手成功；签名器对象仍要求调用者保持不可变。

测试覆盖错配公钥、无效签名器、畸形/typed-nil 密钥，以及不依赖 Leaf 缓存。
配置入口也用错配私钥确认在启动服务前拒绝。最终 server/leader/election
race 五轮通过（29607，14.200/9.085/5.581 秒），相关 vet 和 diff 检查通过。
默认入口仍关闭，无集群变更。当前远端三条 CI 只验证 8a667c24，不包括本修复。

## 完整 server 与真实 TLS 网络的租约接棒测试

新增 `TestPeerRetirementFullServerNetworkLeaseHandoff`，在本机创建两个完整
server，各自使用独立 backend，共享 memkv 测试存储。两个 peer HTTP/2 TLS
listener 使用不同 holder 密钥；公共请求走实际 TCP 上的 mTLS gRPC，并安装
真实 ClientServerOptions 和客户端服务。先让旧 server 完成初始化/ready，
通过公共 RPC 创建租约、写入绑定键并收到 KeepAlive 成功响应，然后启动
另一个 server，确认其观察到原 leader，才拒绝旧节点选主锁的 Get/Update。

启用实验构造器时，新 server 通过真实 peer 请求、存储条件释放和正常选举
接棒，并完成修订/租约等初始化。公共 TTL 和 Range 验证租约及绑定键仍在；
默认 NewServer 对照不发送请求，短观察期内不会越过仍有效的持久选主记录。
第一轮单例 race 通过（43059，1.910 秒）；加入默认关闭对照后，server/
leader/election race 五轮通过（34110，28.531/9.378/5.519 秒），vet/diff 通过。
随后补充实际 HTTP/2/已验证 TLS 以及公开成员/任期字段断言，结果另记。

边界：注入仅拒绝旧选主锁 Get/Update，不是隔离所有 TiKV RPC；使用 memkv
及测试 ClusterIdentifier，不是 TiKV 网络故障。持久租期 30 秒，但本机测试
RenewDeadline 500 毫秒、RetryPeriod 50 毫秒，未改集群原 30/25/0.5 秒配置。
成功的 KeepAlive 响应收到后即关闭该测试流，接棒后另建客户端验证状态，
所以它不证明原“已过期且仍等待”的同一 KeepAlive 流恢复。这个缺口及真实
专用集群原 30 秒总门限仍然开放，不能用本测试取代。

补充断言后的最终五轮 race 通过（73124）：server 28.728 秒、leader 9.406
秒、election 5.696 秒，相关 vet/diff 通过。释放请求实际经过 HTTP/2 已验证
mTLS，接棒后的公开 RPC 保持 ClusterId、改变 MemberId 并提升 RaftTerm。
测试清理关闭客户端、TCP/gRPC/HTTP listener、server 及 backend 工作线程，
没有创建集群资源。本轮本地提交，不推送取消仍在运行的 8a667c24 构建。

## 将接棒测试的故障扩大到旧节点存储接口

完整 server 网络测试现不再只拒绝选主锁 Get/Update，而在旧 backend 下安装
显式存储故障夹具：拒绝读、时间戳、分区查询、扫描、迭代器推进、删除及
批次提交；不暴露 UnwrapKvStorage，避免可选能力发现绕过故障。元数据查询
和资源关闭仍允许，已越过检查的在途调用不保证被取消。新节点共享同一
memkv，但不经过旧节点故障夹具；接棒成功时仍断言旧节点故障保持启用。

初版夹具直接从 Commit 返回错误，遗漏 memkv 从 BeginBatchWrite 起持有
的互斥锁释放，导致回归 96842 在 120 秒超时（leader/election 自身通过）。
这是测试夹具错误，不能作为产品失联结果。修正为在底层批次最先注册
Atomic 拒绝回调，通过实际 Commit 的中止路径释放锁，阻止后续回调及
暂存写入生效。独立用例覆盖故障前已创建的批次、已打开的迭代器、九个
拒绝点、回调未执行，以及恢复读取后原值未变。

修正后定向 race 通过（56074，4.045 秒）；server/leader/election 五轮
race 通过（72894，28.712/9.448/5.595 秒），随后 vet 和 diff 检查通过。
启用实验入口的真实 mTLS 接棒与默认关闭对照均保留。仍是 memkv 接口
故障，不是真实 TiKV 网络隔离，不覆盖原挂起 KeepAlive 流或原 30 秒门限。
本轮未改变产品默认行为、集群配置或部署镜像。

另核实远端 `8a667c24ee995e475a3607c4b7c9333293e1b8aa` 的 backend
CI `35319131334` attempt 1 成功。日志中两次
`TestRealTiKVRetiredRelease` 通过，两次协议运行均为 result=0、
cleanup_failed=0，PD/TiKV 启动中断清理均记录 resources_absent=true。
日志保存在私有目录 `retirement-storage-fault.9yvOvymf/backend-8a667c24.log`，
SHA-256 为 `dc4103c442e1e32ee2124fb059d843265d1e1312a8665b2740a10398b2bd2af9`。
这不覆盖该 SHA 之后的本地修改；镜像和 probe CI 本轮检查时仍在运行。

## 未解决：旧存储持续失联时原过期 KeepAlive 流不能发现接棒者

在 b9c565b3 上扩展完整 server 测试，开启真实 peer gRPC 转发（HTTP/2
mTLS，与退任 HTTP handler 共用监听器），保留原公共 gRPC KeepAlive 流。
授予 TTL=1 的租约并确认初次续租 ACK；启动另一节点并注入旧节点存储故障，
等待两秒自然超过该 TTL，再沿同一流发送一次续租。测试服务端计数确认
旧任期内已收到第二条消息，随后没有提前返回。RenewDeadline 使用本机
四秒配置，以在同一任期内覆盖过期等待；这仍不是集群原 30 秒验收。

定向 race 复现 **失败**（50563，20.246 秒）：接棒者成为 leader 且 ready，
新节点公共 TTL/Range 验证租约及绑定键存在，集群身份/新任期断言通过；
原连接只有一个 KeepAlive 流、两条入站消息（初次 ACK 请求及待恢复请求），
但后者在自创建租约前开始计时的十秒上下文内未收到成功响应。旧存储故障
仍开启，旧节点缓存 leader 仍是自身地址而不是接棒者。没有增加截止时间、
恢复存储、重建客户端或用 TTL=0 当作成功。

源码定位：`etcdproxy.updateClient` 使用 `GetLeaderInfo` 决定目标，失败后
`refreshFailedLeader` 调用 `RefreshLeaderInfo`；后者依赖旧节点已失联的
存储路径，失败时继续保留旧地址。已有退任协议只负责条件释放，不提供
接棒路由发现。因此“新节点已恢复”与“原入口的在途请求恢复”是两个不同
条件，当前实现只证明前者。对照本地 etcd 的 `LeaseRenew`：退任后通过
`waitLeader` 和成员 PeerURLs 寻找 leader；不能假定 KubeBrain 的存储轮询
在同样故障下仍可承担这一职责。

测试草稿保留于工作区 `peer_retirement_network_test.go`，新增用例尚未通过，
**未提交为已通过的回归，也未跳过失败断言**。草稿和完整日志另存私有目录
`retirement-pending-stream.ccAJP7by/`。日志 SHA-256：
`944597a6fb9c9695bdc0fe78f5acc2a893dec820b0463a0f5bbfdae7cdb85450`。
原有完整 server/default-disabled/存储夹具用例单轮 race 仍通过
（83009，4.028 秒），随后 vet/diff 通过；不能据此称全套测试通过。

夹具同时关闭两个 campaign 时，两次 Close 各耗尽五秒 successor 等待预算，
日志保留该结果，工作线程及监听器继续走 Close/Stop 清理；这不是干净的
单节点滚动退出验收。早期复现还修正了夹具把“可代理的 follower ready”误当
“成为新 leader”的判断，现在同时要求 IsLeader 与 requestPathReady。

后续需补足不依赖旧节点存储可达性的、经过认证且有界的接棒发现。不能把
释放请求的 204 当作对方已经成为 leader，不能据此重新激活旧任期，也不能
把未经验证的地址写入路由。原请求恢复和真实 TiKV 故障验收仍未完成。
本轮没有修改产品代码或集群资源。远端 8a667c24 probe CI 35319131319
已返回 success，镜像 CI 35319131308 仍在运行；均不包含本地后续修改。

## 接棒发现协议组件（尚未接入）

新增内部 `peer_successor_discovery.go`，只读探测固定 peer 的本地 leader
就绪状态，**尚未注册到 server handler 或接入 proxy connector**。此阶段
不改变运行行为，上一节的原 KeepAlive 流回归草稿仍失败，不能称问题已修复。

客户端复用严格验证的 retirement HTTPS 传输，但使用独立
`/internal/successor/v1` 路径。构造时要求每个固定 URL 到 holder 的完整、
精确映射，拒绝未知成员、自身成员、多余或缺失的地址；映射复制后不受调用者
修改影响。一次搜索最多探测每个已配置目标一次，共享一个截止时间，单飞、
每秒最多四次且 burst=1，不接受重定向，不解析响应正文或远端提供的地址。
成功必须同时满足 204、唯一且匹配的实例/holder 响应头、正常 CA/主机名 TLS
校验，以及该目标 holder 的 SPKI pin。退任接口的普通 204 不满足此协议。

接收端复用已绑定存储范围的认证和并发/速率预算，认证先于消耗预算；仅接受
无正文 POST，设置读写 deadline，调用非阻塞、无存储 I/O 的本地 readiness
快照，不调用条件释放或修改选主记录。未来接入必须检查本机任期 freshness
和初始化就绪，不能把 follower 的代理可用状态当作 leader 就绪。

返回值只是一条转发候选地址，不是选主授权或稳定的 leader 证明。探测之后
可能立刻退任，实际 peer 请求仍须经过任期检查、认证和事务 fencing。后续
connector 接入还须限制回退触发条件、保留普通入口默认关闭、处理缓存失效
及原上下文取消；不能把候选写入选主状态或取消旧任期已退任的事实。

HTTP/1 与 HTTP/2 真实 mTLS 用例覆盖就绪/非 leader、发送者和接收者 pin
不匹配、错误范围、裸释放 ACK、重定向、错误成员响应以及调用者映射修改。
另外覆盖未知/缺失/多余/自身映射、取消、并发单飞、立即重试限流、超时和
多个目标共享截止时间（首个超时后不请求第二个）。最终五轮定向 race
通过（39331，4.159 秒），随后 server vet/diff 通过。没有运行或宣称完整
server 套件成功，因为原流恢复用例仍是未解决的失败草稿。

## 实验转发入口已接入发现，原过期续租流本机回归通过

增加显式可选 `PeerRetirementConfig.SuccessorHolders`。只有非 nil 且完整
验证的 URL/holder 映射才注册 peer-only 发现 handler，并给单一 proxy
connector 使用 `successorRoutingView`。普通 NewServer、未提供该映射的
实验构造器、client/info HTTP 入口均不启用发现。接收端同时检查本地
EpochAndLeadingFresh、未 draining 和 leaderServing，不使用 follower
代理就绪作为 leader 证明。

路由视图只在权威记录刷新失败且本机已经非 leader/非 fresh 时探测候选。
权威读取最多使用 100 毫秒或剩余预算的一半，为探测留出同一上下文中的
时间；底层仍须遵守取消。候选缓存有效期两秒，本机领导权、epoch、已观察
任期或 holder 变化都会使其失效，存储刷新成功也立即清除。探测期间变化
亦拒绝发布候选。该视图仅传给 proxy connector：Campaign、revision sync、
PeerService 公开选主信息和 backend 写入 fence 全部保留原 election，
绝不将候选写入权威选主状态。健康检查和实际 RPC 仍保留原 peer 校验。

发现启用且转发打开时，启动前额外拒绝无 ProxyTLS、明文回退、跳过验证、
动态证书回调和错误本机 holder 私钥，使用复制后的严格 TLS 配置。
尚无 CLI 启用或集群部署；发现 pin 校验不等于新增了每次 gRPC 连接的
逐目标 pin 绑定，后续凭据/监听器部署审查仍需明确其信任边界。

原先失败的 `TestPeerRetirementPendingExpiredStreamDuringStorageFailure`
首次通过（7038，15.584 秒）。最终用例保留原十秒请求上下文、真实 TTL=1
自然过期、四秒本机 RenewDeadline、旧存储故障持续开启，一个公共流和
两条入站消息的限制。补充新 leader peer gRPC 实际收到续租消息计数，
确认成功响应经过转发，而非客户端新请求或旧 leader 本地续命。

补充响应头断言曾失败两次（45434/61954）：错误地预期原入口响应的
MemberId/缓存 RaftTerm 必须立即等于新成员直连响应。检查当前拦截器和
本地 etcd header.fill 后，MemberId 应标识原入口成员；本实现在旧存储
不可达时仍使用旧入口缓存任期。最终断言固定 ClusterId、原入口 MemberId、
任期不倒退，并用新 peer 入站计数证明转发，未改产品响应头来迎合测试。
**这不证明原入口立即展示最新任期**；该元数据传播差距仍待单独对照分析。

路由失效/探测竞态/预算和发现协议定向 race 五轮通过（98350，4.548 秒）。
第一次广回归三轮通过（73055）：server 58.277、etcdproxy 14.059、leader
6.045、revision 22.722、election 3.769 秒，vet/diff 通过。最终加入 TLS
拒绝、入口隔离和 peer 接收计数后，定向测试通过（66310，15.869 秒），
完整 server/service 子包/election 单轮 race 再通过（13129）：16.707/
5.338/2.738/8.373/2.024 秒，随后 vet/diff 通过。原流用例现在纳入正常
回归，不再是失败草稿，也没有 skip。

边界仍保留：这是 memkv 存储接口故障，不是专用集群真实 TiKV 网络隔离或
原 30/25/0.5 配置验收。夹具整体关闭时的 successor 等待截止被单独记录，
不能当作平滑滚动退出成功。远端 8a667c24 三条 CI 已均报告 success，
但其镜像审计及后续本地提交的 CI/镜像验证尚未完成，本轮未部署。

## 实验转发连接增加逐目标成员 pin

补齐发现之后的 gRPC 连接身份边界：`successorRoutingView` 现在为 connector
提供 `ProxyTLSForEndpoint`。每个固定目标（包括初始权威路由）生成独立 TLS
配置，正常 CA/主机名验证后再校验该 URL 对应 holder 的 SPKI；原入口自身
身份仅允许使用其本机 holder pin。未知地址、provider 错误、nil TLS、跳过
验证及明文回退全部拒绝，不退回普通 CA-only 配置。普通 proxy 没有此可选
接口，原配置行为保持不变。连接重建时仍使用绑定目标的 VerifyConnection，
不会仅凭之前 HTTP 发现成功就信任随后连接的任何同 CA 成员。

首次接入错误地复用了要求 HandshakeComplete 的 HTTP 认证入口，导致正确
证书也被拒绝，定向握手用例 27985 和原流用例 89142 均失败。Go TLS 的
VerifyConnection 在证书验证后、握手完成标志发布前执行，因此抽出共用的
已验证证书链/pin 检查：仅 TLS 回调使用该入口，HTTP 请求仍必须经过要求
完整握手的原包装。未关闭证书链、域名、证书有效期或 pin 校验。

新增真实握手对照先证明另一 holder 的证书可通过普通 CA 和相同 SAN 校验，
再确认逐目标配置拒绝它，同时正确 holder 通过。还验证配置克隆不修改共享
TLS、未知地址拒绝，以及 connector 对 provider 错误/nil/跳过验证/明文
回退均不降级。修正后握手及原过期 KeepAlive 流定向 race 通过（88404，
10.824 秒）；server、etcdproxy、leader、revision、election 三轮 race
通过（19674，52.985/13.982/6.219/22.627/3.770 秒），随后 vet/diff 通过。
默认入口和集群部署均未变化。

另外已下载核对远端 8a667c24 的 probe CI 35319131319 日志：checkout SHA
一致，实际包含 retirement HTTP/1/2 认证结果、lost-ACK、存储 round-trip
及同 holder 再获权后的 replay 测试通过记录。私有日志
`successor-endpoint-tls.3Xg52qGa/probe-8a667c24.log` 的 SHA-256 为
`59c6e534b5c1290ec573aaccbf34bf44586639aa22ae67a082cc438516e95480`。
该 CI 不包含本节及此前本地新提交；镜像独立审计和真实 TiKV/原门限验收
仍待完成，不能将这些本机回归写成生产就绪。

## 原客户端流跨接棒候选缓存有效期持续续租

原过期 KeepAlive 流恢复用例现在收到第一次成功响应后，继续沿同一流每
200 毫秒发送续租，持续至少三秒，跨过路由候选两秒有效期。测试保持原
十秒上下文，不恢复旧节点存储，不重建客户端或流；TTL=1 的短租约每次
响应必须为正。公共流数量仍为一，入站消息数精确等于已发送消息数，新
leader peer 必须收到全部续租，并要求其发现 HTTP 请求计数再次增加，
避免只证明一次接棒、却没有真正经过缓存过期后的重新发现。

初次扩展定向 race 通过（54243，18.643 秒）；增加重新发现计数后，server/
etcdproxy/leader/revision/election 三轮 race 通过（20183，73.407/14.113/
5.947/22.675/3.751 秒），随后 vet/diff 通过。本轮没有产品行为修改。
这仍是两个节点、固定接棒者的本机 memkv 接口故障，不证明第二次 leader
变化、长时间抖动、真实 TiKV 网络分区或原集群 30 秒验收门限。

另对固定 8a667c24 创建独立发布证据目录
`release-8a667c24.hdz6oIhL`；三条 CI 的 exact SHA、attempt 1、workflow
路径、具体测试及中断清理结果核验成功（80031），回执为
`ci-evidence.2OSNMiyf/verified.json`。独立镜像审计已启动（40505），结果
须在终态后另记；这些证据不覆盖之后的本地提交。

该镜像审计随后成功（40505，终态 0），固定回执
`image-evidence.XhLfZkck/verified.json`：index
`sha256:9ff6c7e1604a274d1b6951b4cba9f28f05e0f05417040620c1a0ebb7e46efe70`，
amd64 `sha256:8d256d278bcd68674c708c9fa919d094298e4b8db8492c65493cbe52dca270a4`，
arm64 `sha256:1e74d2a9dbbb01b9716a22d43c3003dfe412404e064c75894b7f5add8b10a6dd`。
检查固定源码归档对应的 OCI/version、Go/module/fork/grpc 和非 root 身份；
只执行 amd64 version，arm64 只检查索引身份。所属临时容器与提取二进制
已经清理，独立复查不存在；这些临时副本可从镜像重新提取，未删除镜像。
没有部署或生产验收结论。

## 避免首个阻塞候选耗尽每轮发现预算

新增反例确认固定顺序发现的可用性缺口：首个目标不响应时，原实现将整轮
总预算交给它，后续健康目标根本未被访问。定向 race 在原实现失败
（73816，0.526 秒，返回 peer successor unavailable），不能依靠下一轮
重复同样的固定顺序自行恢复。

现将“剩余总时间 / 剩余目标数”分配为本次探测的子截止时间。每次请求
仍是一次尝试、共用原总上下文，结束即取消子上下文；过子截止时间返回的
结果亦不接受。首个目标失败后，后续目标有保留时间，而不是增加总超时。
单飞、限流、固定成员、mTLS/pin、无重定向和无响应正文语义均保持不变。

测试覆盖首个独立成员阻塞、后续正确 holder 就绪时能发现后者；四个目标
均阻塞时全部得到一次探测机会，但共享 200 毫秒总预算，不变成每目标
各 200 毫秒。原来“首个阻塞后不应触及第二个”的用例改为验证预算分配
后可访问第二个，同时仍拒绝其不带身份声明的裸 204；这是修正分配策略，
不是放宽成功判定或集群验收门限。

定向五轮 race 通过（90864，5.587 秒）；最终 server/etcdproxy/leader/
revision/election 三轮 race 通过（21382，64.761/13.977/6.017/22.749/
3.921 秒），随后 vet/diff 通过。包含原流恢复及跨缓存过期持续续租用例。
少量本机目标得到时间片不证明所有实际网络延迟下均可及时发现，也不覆盖
成员数上限场景的原 30 秒门限，仍需真实集群测试。

远端 5b8f6af7 的 probe 35323093168、image 35323093221 本轮均核实为
in_progress，未重复触发或取消。检查 workflow 后确认 probe 会完整执行
server 和 etcdproxy 的 race 测试，不会因测试名筛选漏掉新接棒用例；但
这两条构建不包含本节的后续修复，本轮仅本地提交，未推送打断现有构建。

## 实际 peer HTTP 监听器补齐已验证 TLS 身份传递

检查部署接入时发现：外层 `identityTLSListener` 已验证 mTLS 并注册连接身份，
但 peer HTTP transport 没有像 client transport 一样传入身份注册表；而 TLS
已在外层终止，内部 net/http 的 Request.TLS 为 nil。此前 httptest 直接终止
TLS 的测试无法覆盖这个实际监听器边界。

先使用真实 secure listener/cmux/net/http 组合复现：要求已验证 TLS 身份的
peer handler 在 HTTP/1 和 HTTP/2 两种子用例均返回 403（98693，0.152 秒）。
现在两个 peer HTTP 构造分支共用 `newPeerHTTPTransport`，通过 ConnContext
传递连接注册表状态，再仅在 Request.TLS 缺失时克隆请求并恢复该状态。
不读证书转发头，不覆盖原生 TLS 状态，不把没有 VerifiedChains 的连接
升级为已认证连接；具体范围/holder/pin 授权仍由内部协议 handler 执行。
client/info 路径不变，本轮未注册任何实验接口或新增命令行开关。

测试使用临时真实 CA 和双用途证书，验证传递后的客户端证书 DER 精确相符，
并验证 ResponseController 读 deadline 在实际 peer HTTP handler 可用。
补充头部伪造、原生 TLS 优先、不制造验证链、原始请求不被修改的反例。
首次修复后的测试 56133 曾因 ALPN 协商得到 HTTP/1 而非预期 HTTP/2 失败：
现有监听器在双方提供两个协议时优先 HTTP/1。测试改为每个子用例只提供
所测协议，没有修改产品 ALPN 顺序，也没有把 HTTP/1 结果写成 HTTP/2 成功。

最终定向三轮 race 通过（47746，1.256 秒）；endpoint/transportidentity/
server/cmd-option 完整单轮 race 通过（91172，15.548/1.029/25.854/1.225 秒），
随后相关 vet/diff 通过。临时监听器、客户端和证书目录按测试生命周期清理。

部署接入尚未完成：Endpoint.Run 仍调用普通 NewServer。实际端点 TLS 使用
动态证书/CA/CRL 校验回调，而实验发送器明确拒绝这些回调；不能为启用实验
而静默删除轮换或撤销策略。下一阶段必须定义受控的凭据适配及配置校验，
同时明确连接老化模式下控制 HTTP 的协议边界，再接命令行入口及独立 holder
证书。旧集群、已有共享证书和默认开关未改动。两条 5b8f6af7 CI 本轮最后
检查仍 in_progress，本修复未推送，不能归入它们的验证范围。

## 凭据适配准备：只传递重新加载的材料，不接受任意 TLS 回调

新增 `transportidentity.ClientCredentialSource/ClientCredentialMaterial` 作为
数据边界，包含证书/私钥对象、CA 池、可选解析后的 CRL、服务名和 TLS 参数，
不包含 GetClientCertificate/VerifyConnection 等任意函数。新增 endpoint
内部 `peerCredentialSource` 捕获操作员提供的文件路径和策略，每次 Load 都
重新读取内容，不缓存上一份成功材料；支持独立出站 client cert/key，未配置
时使用 peer cert/key。调用方修改 SecurityConfig 不会改写已创建来源的路径
和策略，返回的证书、CA、CRL 和 cipher slice 均由本次加载独立拥有。

来源要求 TLS-only、明确 ClientAuth 和 CA，拒绝混合明文模式、不完整的
出站密钥配置及低于 TLS 1.2 的版本组合；加载时解析匹配的 X509 key pair，
检查本机叶证书有效期、CA PEM 和 CRL DER。错误统一返回不含材料内容的
错误，不退回旧证书。材料加载**不是对端认证**，CRL 的实际撤销判定、
链/主机名验证、本机 holder 归属、远端逐目标 pin 仍须由后续适配器实施。

文件加载使用非阻塞 open，并对实际打开的对象要求普通文件；允许 Kubernetes
Secret 的符号链接，但拒绝 FIFO、设备和目录。PEM/密钥上限 1 MiB，CRL
上限 16 MiB，读取时仍限制字节数，避免只依赖打开前 stat。上下文在阶段间
检查，**不宣称能中断内核中阻塞的普通文件 I/O**，也没有用后台 goroutine
掩盖这种限制。

测试覆盖同一来源重复加载证书、根 CA 和 CRL 的变化、移除旧 CA 后不再信任
旧证书、返回值修改隔离、错误内容不泄漏、失败无旧材料回退、取消、独立
client key pair、Secret 符号链接及非普通/超大文件拒绝。定向三轮 race
通过（82757，1.179 秒）；endpoint/transportidentity/server/cmd-option
完整单轮 race 通过（20414，15.523/1.029/25.516/1.231 秒），vet/diff 通过。

这个材料来源尚未接到实验 sender/proxy，未替换现有端点的动态 TLS/CRL
回调，未启用命令行入口或改变集群。下一步必须在受控适配器中以每次新加载
的材料执行完整校验，并覆盖真实握手中的轮换、撤销、错误 pin 和会话重连，
之后才能把配置接入部署；不能仅凭材料加载测试宣称轮换链路已经完成。

## 重新加载材料的完整校验及真实会话恢复验证

新增服务端内部材料校验器：每次重新解析本机叶证书 DER，检查有效期、
私钥公钥匹配和本机 holder 的 SPKI pin，不信任 tls.Certificate.Leaf 缓存。
远端校验使用本次材料的显式 CA 池重新构建链，验证服务名、serverAuth、
有效期、TLS 版本和指定 holder 的 pin；旧 ConnectionState.VerifiedChains
不作为信任来源。操作员配置的 ServerName 作为显式名称策略保留。

可选 CRL 要求叶证书签发者的有效签名、匹配的 issuer 和当前有效时间窗口。
这是实验性的单份完整 CRL 策略：增量、分区、间接及未知关键扩展均拒绝，
不支持多签发者的完整撤销链管理。当前按链内 serial 保守拒绝匹配项，
可能拒绝不同签发者下序列号相同的证书；不把这种保守策略描述成通用 PKI
撤销实现。现有 endpoint CRL 行为未改变。

测试包含旧验证链不绕过新 CA、错误名称/holder/版本/有效期、签名错误、
过期/未来/撤销 CRL、非支持扩展，以及私钥和缓存叶证书检查。
新增真实本机 mTLS 握手测试明确断言 TLS 1.2 DidResume：首次握手和正常
恢复成功，移除 CA 后恢复握手失败，重新建立会话后加入撤销记录，恢复
握手再次失败。每次拨号有三秒超时。最终定向三轮 race 通过
（51982，1.466 秒）；这不是 TLS 1.3 恢复或集群证书轮换验收。
加入拨号超时前的 server/endpoint/transportidentity 完整单轮 race 通过
（73442，25.765/15.611/1.028 秒），随后相关 vet/diff 通过；超时修改后的
验证范围是上述定向三轮，不把修改前结果表述为最终文件的完整回归。

校验器尚未接到生产 sender/proxy；测试直接调用受控 VerifyConnection，
不能据此宣称文件轮换到实际部署的端到端链路完成。后续仍须实现每次握手
加载材料的适配器、已建立连接的重验/淘汰策略、命令行配置以及独立成员证书。
默认端点、现有 TLS 回调、集群配置、原验收门限均未修改。

远端 5b8f6af7 的 probe CI 35323093168 已成功，原始日志确认完整服务端
租约接续、存储故障中的原流续租、逐端点 TLS pin 和禁止 TLS 回退测试通过。
日志 SHA-256 为 cebe4198dc510d855bf50a63edcc0f79e98f3b10d355d833a19cf85648f2477f，
位于私有 peer-reloaded-verification.a5VUp97u 目录。该 CI 不包含此后的
本地凭据适配修改；镜像 CI 35323093221 最后检查仍在运行，未重复触发或部署。

## 受控新连接握手适配器

新增内部 `handshakeReloadedPeer`：接管调用方已连接的 socket，要求带截止时间的
有效上下文，每次握手从 ClientCredentialSource 加载新材料并检查本机 holder。
构造独立 TLS 配置，保留标准 RootCAs/ServerName 验证，再通过 VerifyConnection
执行当前材料的链、撤销和精确远端 holder 校验；没有 InsecureSkipVerify、
继承的任意回调或会话缓存。协议列表和 cipher 列表复制，失败均关闭 socket，
错误不携带来源错误或证书材料。正常 TLS 的 VerifiedChains 保留，供后续 HTTP
响应身份检查使用。

真实 mTLS 测试从服务端记录客户端证书序列号，验证下一次握手确实使用轮换后的
证书；覆盖移除 CA、加载错误、错误本机/远端 pin、错误主机名以及恢复后的新加载。
另覆盖无界/取消/nil 上下文在加载前拒绝，和不响应的 net.Pipe 对端按截止时间
退出并关闭 socket。最终定向三轮 race 通过（28950，1.512 秒）。
最终 server/endpoint/transportidentity 完整单轮 race 通过（42771，
21.003/15.543/1.030 秒），随后相关 vet 和 diff check 通过。

对照本地 etcd 5cd9f4ee13801e18825d661e5005ae599460bc3a 的
client/pkg/transport/listener.go：baseConfig 的 GetClientCertificate 会重新
读取客户端证书，ClientConfig 构造时读取 CA 池。这里保留新握手使用当前
客户端凭据的目标，并针对实验控制通道额外每次加载 CA/CRL；不声称这是
etcd 原生逐成员 SPKI 机制，也未改变 etcd 客户端可观察的数据语义。

这一适配器仍待 sender/discovery/proxy 调用接入。当前每条新连接采用独立握手，
不复用 TLS session cache；并未解决已建立的长连接何时重验和淘汰，也不宣称
取消能中断普通文件的内核 I/O。端点默认行为与集群未改变。镜像 CI 35323093221
最新检查已完成 success，源码仍是 5b8f6af7，不包括这批本地修改；镜像身份审计
及新版发布、真实 TiKV 故障门限验收仍未完成。

## 可选控制 HTTP 材料源接入

实验配置新增 `ControlCredentialSource`，必须同时提供完整 SuccessorHolders；
未提供时原路径不变。启动阶段仍要求原 TLS 参数通过静态身份校验，新来源只
用于退休通知和 successor 探测，不替代独立 gRPC ProxyTLS。尚未接命令行，
不能用此字段声称完整端点轮换已经部署。

新路径将两类控制请求接入受控握手适配器。每次请求新建 HTTP/1 transport，
禁用 keep-alive，不协商 HTTP/2；这是控制通道的明确协议选择，防止连接池
或多路复用跳过下一次材料加载。目标必须精确匹配操作员配置的 HTTPS URL
及两个内部路径之一，只接受 POST，不接受额外查询串或 Host 改写。握手
之前绑定目标 holder，错误远端 pin 会阻止请求正文发出，而不是等 ACK 后
才发现错误成员。保留禁止重定向/环境代理、8 KiB 响应头限制及原全局预算。
拨号与握手绑定原请求上下文，因为标准 Transport 的拨号上下文可脱离请求；
请求正文在前置拒绝时也关闭，失败连接由握手适配器回收。

真实控制请求测试同时走 send 和 discover：服务端实际观察到轮换后的客户端
证书，每个请求各加载一次；移除 CA、来源错误、错误本机或远端 pin 时两类
请求均不进入 HTTP handler，恢复材料后成功。服务端支持 HTTP/2，但测试
断言此路径实际为 HTTP/1；另验证未配置路径和查询串拒绝、配置映射复制以及
来源缺少目标配置时启动拒绝。此测试不替代文件投影轮换、长期并发负载或真实
TiKV 故障验收；长连接重验、gRPC 接入和独立成员证书部署仍开放。

最终定向三轮 race（含错误远端 pin）通过：72445，1.651 秒。加入该最后
测试用例前的 server/endpoint/transportidentity 完整单轮 race 通过：6051，
20.843/15.524/1.027 秒，随后相关 vet/diff 通过；生产代码在这两次验证之间
未变化，不把较早全包结果说成包含新增测试用例。

## gRPC 客户端凭据适配准备

将材料加载和 TLS 配置构造提取为共用内部函数，HTTP 路径保留原行为。
新增 client-only 的 TransportCredentials 适配器，每次 ClientHandshake
使用当前材料，绑定固定本机/远端 holder 和主机名，并受配置预算及调用方
上下文共同限制。加载、TLS 或 ALPN 失败均关闭底层 socket；不返回材料
来源的原始错误。Clone 复制配置，不提供运行时 OverrideServerName，也
不允许误用为服务端凭据。此适配器尚未接入代理连接器。

已检查当前依赖 grpc-go v1.83.2 的 credentials/tls.go ClientHandshake：
它会用 authority 覆盖 config.ServerName。因此先验证调用方 authority 的
主机名等于固定目标，再把操作员名称策略交给标准 TLS 凭据实现，不能仅设置
config.ServerName 后假设它不变。标准 gRPC 包装返回 TLSInfo、
PrivacyAndIntegrity 和底层连接包装；额外要求实际协商 h2，即使库的兼容
开关允许缺少 ALPN 也拒绝。材料中的操作员 ServerName 覆盖策略仍保留，
不是允许 RPC 调用方任意更改认证目标。

测试包含真实 mTLS/h2 握手、TLSInfo 身份、错误 CA/pin/authority、加载错误、
没有 h2 的服务端拒绝，以及同一凭据对象下一次连接重新读取已移除的 CA。
另用真实 gRPC Health Check 验证适配器可完成 RPC，不只是返回模拟 AuthInfo。
它仅覆盖新建连接，不证明既有长流已被定期重验；连接器接入、长连接退休和
CLI 配置仍待完成。未改变测试集群、默认开关或固定镜像。

最终定向三轮 race 通过（54894，1.380 秒）。新增下一连接移除 CA 的测试
断言前，server/etcdproxy/endpoint/transportidentity 完整单轮 race 已通过
（65051，22.181/5.444/15.655/1.029 秒），随后相关 vet/diff 通过。
两次验证之间生产代码未改；最后新增的断言只计入定向验证。
e994f2c3 的镜像 35326113519 和回归 35326113607 最后检查均 in_progress；
本轮提交不在该 CI 源码范围，未推送取消运行中的构建。

## 动态 gRPC 凭据接入代理连接器

实验配置新增独立的 `ProxyCredentialSource`，要求显式 SuccessorHolders。
只有设置该来源的路由视图才实现 ProxyCredentialsForEndpoint；未启用的普通
代理和原静态路径不受影响。动态视图仍保留原选举对象、发现提示及静态 TLS
配置检查，目标必须来自固定成员映射（或本机精确身份 URL），不得从响应中
增加认证目标。返回的凭据绑定本机、远端 holder 和 URL 主机名。

连接器取得动态凭据后使用 client/v3 DialOptions 覆盖默认 transport credentials，
同时设置其固定 authority。已检查依赖 client/v3 的拨号代码：调用方 DialOptions
在内部默认 transport credentials 之后追加；新增完整网络测试验证了实际生效，
不只依赖选项顺序推断。提供者错误、nil、非 TLS、空 authority 或明文 fallback
配置均拒绝，不退回原静态凭据。原静态 TLS 启动校验仍要求有效初始配置，
尚未移除这个双配置阶段，也不允许用动态来源绕过其安全检查。

完整双服务端测试增加动态代理模式：旧成员存储接口故障、租约自然到期后，
原公共 gRPC 流向 successor 接续，继续跨过发现提示 TTL 续租；原所有断言保留，
并明确断言材料来源实际被调用。控制 HTTP 在这项测试仍走原 mTLS/HTTP2，
只有代理启用动态来源。定向 race 通过（22145，18.931 秒）；同进程后续
连接器拒绝回退的三轮 race 通过（1.111 秒）。逐端点 TLS 测试同时验证动态
提供者：同 CA/SAN 的错误 holder 被拒绝，未配置 URL 被拒绝。

这仍是共享 memkv 后端及真实网络协议的本机测试，不是独立 TiKV/PD 网络故障
或原 30 秒门限验收。每次重连可加载材料，不代表已建立的连接能及时感知
证书撤销；长连接重新校验/退出策略、真实文件来源到 Endpoint.Run 的接入、
CLI 和独立成员证书部署仍未完成。集群未改动。

server/etcdproxy/endpoint/transportidentity 完整单轮 race 通过（97387，
38.809/5.476/15.724/1.030 秒），随后相关 vet/diff 通过。最后补充的配置
来源绑定断言及逐目标测试三轮 race 通过（95968，1.369 秒）；生产代码未变。
镜像 35326113519 和回归 35326113607 对应 e994f2c3，最后检查仍 in_progress，
不包含本轮本地提交，未重复 dispatch、推送或部署。

## 动态代理连接的有限信任寿命

动态 gRPC 凭据模式现在将每次成功握手返回的连接包装为有限寿命连接。
从开始加载材料计时，最长 30 秒；本机叶证书或已验证远端链的任一证书
更早到期则使用更早时间。仅对显式 ProxyCredentialSource 模式生效，
普通端点和静态代理不变；这个 30 秒是实验信任刷新周期，不是对原外部
故障验收门限、选举 LeaseDuration 或客户端 deadline 的修改。

到期计时器关闭空闲或活跃连接，后续读写也检查到期状态，不能靠持续流量
无限维持旧信任。下一次 gRPC 握手重新加载材料；提前 Close 停止计时器，
并发关闭只执行一次底层 Close，保留原连接的 syscall.Conn 能力。没有
额外的定时文件读取 goroutine，因此不会因内核阻塞的文件 I/O 堆积后台
重验任务。计时器调度和底层 Close 仍受运行时/网络影响，不是硬实时保证。

这是强制重新握手策略，不是在原连接上无损轮换：到期会打断尚在进行的
RPC，已经准入的写入可能结果不确定，不增加自动重放。既有代理的不确定
事务不重放规则保留。CA/CRL 文件变化并非立即撤销连接；最长刷新周期内
仍可能使用原验证结果，CRL 的新状态在下一次握手核验。预配置 holder pin
集仍不可变，换新 key 需要先有明确的 pin 轮换配置。生产启用前仍须评估
长流恢复、周期性重连开销和节点间同时到期的影响，不能把这个实验默认值
当作已经完成的生产策略。

测试覆盖空闲到期、阻塞读取释放、已过期连接拒绝、并发关闭、提前关闭停止
计时器和真实 TCP syscall.Conn。五轮 race 通过（22526，1.424 秒）。
真实 gRPC 测试使用短期客户端证书触发更早到期：首次 Health RPC 成功，
随后移除 CA 并提供另一个仍有效且 pin 正确的本机证书，原连接退出，下一次
加载失败且没有第二个 RPC 到达服务端。三轮 race 通过（74389，6.172 秒）。
这不等于真实集群撤销、长期 soak 或原 30 秒故障门限验收。

最终 server/etcdproxy/endpoint/transportidentity 完整单轮 race 通过
（3555，45.726/5.467/15.650/1.028 秒），相关 vet/diff 通过。代理全包
包含既有 TestTxnDoesNotReplayAmbiguousForwardResult 等用例；尚未增加
“此证书寿命计时器恰在真实事务提交后触发”的专项故障用例，不能将既有
不重放测试当作该组合场景的完整证明。镜像 35326113519、回归 35326113607
最后检查仍 in_progress，当前本地修改未推送或部署。

## 连接信任到期时已准入事务的结果不确定性

新增 TestCredentialExpiryDoesNotReplayAdmittedProxyTxn，经真实 etcdproxy、
client/v3、gRPC/mTLS、逐目标动态凭据和连接寿命计时器执行。服务端先增加
副作用计数，再阻塞响应；分别覆盖响应头尚未发送与已发送的时刻。首份本机
证书短期到期触发连接关闭，后续加载返回另一个仍有效且预先授权的本机证书，
因此重连可以成功，不能靠永久断连让“不重放”断言侥幸通过。

原 Txn 返回错误且调用方 deadline 尚未结束，副作用恰好一次；重新 Ready
后仍为一次。随后显式提交一个新 Txn 成功，累计副作用为二，证明新请求
通路恢复而原请求没有自动重放。三轮 race 通过（73972，25.116 秒）；
最终 server/etcdproxy 完整单轮 race 通过（12570，52.966/5.461 秒），
相关 vet/diff 通过。初稿因 EtcdProxy 接口未暴露 Close 编译失败，改为
显式检查具体对象的 Close 能力后再验证，未改生产接口。

副作用计数是“已经执行但响应丢失”的模型，不是 TiKV 事务落盘证据，
也不证明无损轮换或 exactly-once。原请求结果仍可能不确定；应用侧不能
把该错误当作未提交证明。真实 TiKV/PD 提交后断连与 Kubernetes 长流验收
仍开放。

远端 e994f2c3 的 probe CI 35326113607 已失败：Watch race 分组中的
TestWatchInvalidCreateAutomaticIDAndUnknownCancelKeepStreamAlive 在
watch_test.go:975 的响应头 revision 断言得到 0，预期为 seed revision 2。
后续领导权/代理阶段 skipped，不能宣称本批远端回归通过。镜像 35326113519
仍运行。失败日志保存在私有 credential-expiry-txn.FXNmj6Yf/probe-failure.log，
SHA-256 cf595a8f038079f19aaa3e3d16c75867d052002eca61b12134435c806d63a2f0。
正在单独复现此 Watch 失败；未归因于 Runner，也未放宽断言或重复触发 CI。

后续已通过固定“提交完成但事件尚未发布”的窗口复现相同 2/0 差异，并修正
无效范围创建拒绝响应的 revision 来源，详见
[无效 Watch 创建响应头](watch_invalid_create_revision_cn.md)。原 CI 失败记录
仍保留，不能用本机通过追溯改写为远端成功。

## Endpoint API 的显式实验接入

Endpoint.Config 新增默认 nil 的 ExperimentalPeerRetirement；只含 scope、
holder pin、固定端点映射和预算，不接受 TLS 回调或私钥对象。Run 通过
newDataServer 选择普通构造器或实验构造器。实验路径必须使用 TLS-only、
ClientAuth 和明确 CA 的 peer 文件策略，控制 HTTP 与 gRPC 代理共用重新
加载来源。端点映射复制并排序，pin slice 复制，不依赖外部 map 的后续改动。

启动先加载一次材料，构造现有服务端静态身份校验所需的 bootstrap snapshot；
运行时两类出站连接均使用来源重新加载，包含 ServerName/CRL 策略。bootstrap
没有动态回调，不是静默删除普通 endpoint 的回调，而是显式实验模式改用
已实现的受控材料校验。启动加载有五秒上下文预算，但仍不声称它能中断内核
中的普通文件 I/O。后续 scope、pin、URL 和协议预算由实验服务端构造器检查。

Run 的清理改为在构造器之前注册，校验失败也取消上下文并关闭后端；只有
实际创建了 server 才调用其 Close，监听器在构造成功之后才启动。测试覆盖
材料共享、真实文件轮换、保留服务名策略、映射隔离，以及明文/混合模式、
缺 CA、缺目标和取消时不创建 server 并清理后端。定向三轮 race 通过
（14505，1.152 秒）。准备配置测试中的模拟 pin 不被当作服务端身份校验
通过证据；完整启动的 scope/pin 校验由后续构造器承担。

尚无 CLI 开关，未在集群启用。新 Endpoint 实验路径的成功启动、真实监听器
与两节点证书轮换联调仍待覆盖；现有测试不替代这些步骤。控制 HTTP 仍受现有
监听器边界约束，生产前须审查握手/头部期限及连接准入，不能仅因为有 handler
级预算便宣称完整抗滥用策略已经完成。静态默认路径保持不变。

最终 endpoint/server/transportidentity/cmd-option 全包单轮 race 通过
（44106，16.704/48.119/1.030/1.236 秒），随后相关 vet/diff 通过。
镜像 35326113519 最后检查仍 in_progress，probe 35326113607 的 Watch
失败未被远端重验；Watch 修复和本轮接入均未推送或部署。

## 真实 Endpoint 启动和控制路由隔离

新增 TestExperimentalPeerEndpointStartsAndIsolatesControlRoutes，使用带非零
cluster ID 的 memkv 后端、真实单节点选举、文件证书和 Endpoint.Run，实际
启动 client/peer/info TCP 监听器，而不是只直接调用构造器或 handler。
分别覆盖普通复用监听器与启用 GRPCMaxConnectionAge 的 native gRPC 模式；
控制探针明确使用 HTTP/1（native 模式 HTTP/2 由 gRPC 接管）。

等待 peer successor 返回 204，证明服务端完成初始化并接受固定 scope/holder
且具有可信 TLS 身份的请求；同 CA/SAN 但未授权 pin 的客户端收到 403，
没有客户端证书时 TLS 握手失败。peer retirement 在通过认证后对空 ownership
condition 返回 400，不触发后台释放。两个控制路径在 client 和 info 端口
均为 404，不能借助客户端入口调用成员控制协议。临时证书和监听器按测试
生命周期回收，不使用集群凭据。

首次执行的路由断言都通过，但 Close 返回“等待自愿交接 successor 超时”
导致测试失败（1181，5.310 秒）。这个单节点夹具没有可用 successor，
不能要求生产交接策略虚报成功；后续明确断言 context.DeadlineExceeded
并等待 Run 退出，没有改变产品关闭逻辑、忽略所有错误或声称优雅交接成功。
成功交接仍由双服务端测试及后续真实双 Endpoint/集群实验分别覆盖。
两种模式三轮 race 通过（69714，32.110 秒），其中尚未加入最后的 retirement
空条件 400 断言。该测试保留启用代理，没有为了绕过关闭失败禁用代理。

原 e994f2c3 镜像 CI 35326113519 已成功，probe 35326113607 仍是已记录的
Watch 断言失败；不把镜像成功当作该源码回归通过，也未进行镜像部署。

最终代码（含 peer retirement 400 断言）的 endpoint/server/cmd-option
完整单轮 race 通过（82459，26.038/48.877/1.219 秒），随后相关 vet/diff
通过。本轮仍未覆盖两个真实 Endpoint 之间的证书轮换或原 TiKV 故障门限。

## 双真实 Endpoint 的动态凭据转发链路

新增 TestExperimentalEndpointPairForwardsThroughDynamicPeerCredentials：
两个独立 backend/Endpoint.Run 共享带 cluster ID 的 memkv 测试存储，
各自 client/peer TCP 端口、独立密钥和固定 holder pin。先启动第一个成员
并通过公共 Range 确认可用，再启动第二个成员；通过两个公共 Status 响应
确认第二个确为 follower、其 leader 为第一个成员，而不是两个孤立 leader。
随后通过 follower 的官方 client/v3 Put 写入，再从 leader Get 校验 value
和 mod_revision 与 Put 响应一致。共享存储只在两个 Endpoint 全部退出后关闭。

证书只有 peer.test DNS SAN，不含 IP SAN；实际 peer URL 使用 127.0.0.1。
bootstrap TLS 不携带 ServerName，运行时动态材料保留 peer.test。成功的
follower 转发因此也检查了运行时名称策略确实生效，不只验证路由配置被创建。
公共测试客户端必须显式 grpc.WithAuthority("peer.test")：首轮遗漏此配置
被 grpc-go 按 IP 校验而失败（5337），补充身份断言但尚未修正 authority 的
三轮也失败（25266）。没有关闭证书验证或给 fixture 补 IP SAN 来回避该边界。
修正后定向 race 单轮通过（73959，10.302 秒）。

整个夹具退出时保留并检查既有的 successor 等待超时，只允许已知
context.DeadlineExceeded，其他关闭错误仍失败；这不是优雅交接通过结论。
本项是真实两个监听端点之间的正常转发，不是运行中证书轮换、原流跨轮换
恢复、TiKV/PD 故障或原 30 秒验收。入站 CN/hostname allowlist 的现有
校验留在原 server TLS 配置，本轮没有更改该策略，也不声称本测试覆盖其
全部组合。两条 16f9fa1a CI 已从 queued 转为 in_progress，未重复触发。

最终双端点定向三轮 race 通过（99470，28.673 秒），随后 endpoint 全包
单轮 race 通过（35.181 秒），vet/diff 通过。另发现现有 probe 工作流虽被
pkg/** 修改触发，却未运行 endpoint 测试：新增 endpoint/transportidentity
vet 和完整单轮 race 步骤，沿用 self-hosted、失败即停、3 分钟包超时及
30 分钟作业总预算。工作流契约先红测确认旧 YAML 缺少命令，随后补齐工作流。
当前远端 16f9fa1a 的运行不包含这个新步骤，不能回溯称其覆盖 endpoint。
修改后的 build 工作流契约和 transportidentity 全包 race 通过（4280，
2.539/1.030 秒）。本轮仅本地提交，未取消运行中的 CI，也未部署到集群。

## 双 Endpoint 在线出站证书轮换

新增 TestExperimentalEndpointPairReloadsProjectedClientCertificate，复用双端点
正常转发夹具，但 follower 的独立出站 cert/key 指向同一个目录符号链接。
预先配置旧/新两个 key 的 holder pin，运行中通过 rename 原子替换目录链接，
两端 Endpoint 和 backend 均不重启。使用现有 GRPCMaxConnectionAge=750ms、
Grace=250ms 促成真实 gRPC 重连；这只是测试连接老化配置，不改默认的动态
连接信任周期，也不改变集群的选举参数或原验收门限。

leader 的入站 peer VerifyConnection 测试观察器串接在原有校验之后，记录
真正呈现的证书 serial，保留标准 mTLS 校验和原回调。先断言观察到旧证书
201 且尚未观察到新证书 299；切换目录后等待实际新握手呈现 299，然后再经
follower Put、leader Get 核对 value 和 revision。它不是只看本地证书文件
已变化，也不是靠重启构造器重新加载。首轮 race 通过（53054，10.797 秒）。

此用例覆盖预授权新 key 的出站客户端证书轮换、原子目录投影、运行时文件
来源、TLS 握手和新请求转发；不覆盖 CA/CRL 轮换、撤销旧 pin、在途原始
Watch/Lease 流无损延续或零瞬时错误。整个夹具退出时仍可能返回已记录的
successor 等待超时，不将其描述成优雅交接成功。两条远端 16f9fa1a CI
仍在运行，不含此本地测试和上一轮新增的 endpoint CI 步骤。

最终定向三轮 race 通过（66646，30.225 秒），随后 endpoint 和
transportidentity 全包单轮 race 通过（45.829/1.027 秒），相关 vet/diff
通过。证据日志在私有 credential-expiry-txn.FXNmj6Yf 目录的
endpoint-rotation.log 和 endpoint-rotation-repeated.log。未推送或部署。

## 动态 gRPC 连接的 CRL 有效期边界

动态连接原先按 30 秒最大寿命及证书到期时间关闭，但未将本次握手使用的
CRL NextUpdate 纳入截止时间，因此短有效期 CRL 到期后连接仍可能继续使用。
现在 TLS 材料构造同时返回本地证书与 CRL 的最早到期时间；gRPC 再与远端
证书链到期时间、原连接寿命上限取最小值，交给现有连接关闭机制。
签名、颁发者、撤销项及当前有效期校验仍在握手期间执行，没有延长过期
CRL 的有效期，也没有在加载失败时回退旧材料。普通静态 TLS 路径不变。

新增 TestReloadedGRPCConnectionExpiresWithCRL，使用真实 TCP/mTLS gRPC
Health 服务与约两秒有效期的签名 CRL：先确认 RPC 成功，等待旧连接离开
Ready，断言过期材料不能使第二个 RPC 到达服务端；再发布新的有效签名
CRL，确认同一个客户端通过重新握手恢复。恢复阶段只对只读 Health 调用
使用 WaitForReady，不给业务写请求增加重放。定向三轮 race 本轮重新运行
通过（23651，9.166 秒）；随后 server、endpoint、etcdproxy、transportidentity
全包单轮 race 分别通过（51.326/44.923/5.352/1.027 秒），相关 go vet 与
git diff --check 通过，同一执行链终态 0。

这验证内存材料源的 CRL 到期拒绝及更新后恢复，不是实际文件投影轮换、
在途 Watch/Lease 无损恢复或真实 TiKV 故障验收。连接关闭受运行时调度影响，
不是硬实时撤销保证；在途写入结果仍可能不确定，不能自动重放。

收尾时远端 16f9fa1a 的 probe 35328959294 仍在 Race test all probe
regressions 步骤，image 35328959280 仍在 Build and push TiKV test image
步骤。它们不包含本轮修改；没有取消、重复触发或推送，也没有集群写入。

## 实验性 peer 请求头与 TLS 后协议分类的期限

CLI 接入前检查发现，handler 的 read/operation budget 不覆盖读取请求头：
peer 仍继承普通端点五分钟 ReadHeaderTimeout 和默认约 1 MiB 请求头限制。
新增真实 mTLS 套接字回归先在旧实现失败（72643，7.170 秒）：24 KiB 请求头
到达 handler 并返回 204，未完成的请求头直到客户端七秒期限仍未被关闭。

现在仅 ExperimentalPeerRetirement 非 nil 的 peer HTTP transport 使用
五秒 ReadHeaderTimeout 和 16 KiB MaxHeaderBytes；普通 client/info/peer
配置不变。MaxHeaderBytes 是 net/http 的配置限额，解析器可能有读取余量，
不宣称逐字节的内存硬上限。保持整个请求的 ReadTimeout/WriteTimeout 为零，
避免把这些期限变成长时间运行的 gRPC 流的整体寿命。

另外，TLS 握手已完成但尚未发送应用协议字节的连接还未进入 net/http；
对该实验 transport 将五秒期限传入 TLS 内层 cmux 分类阶段。检查实际依赖
cmux v0.1.5 的 serve 实现：匹配成功后清除分类读期限，再转交内部服务。
原 TLS 握手及外层分类的独立期限没有修改。新增测试覆盖 TLS 后静默连接，
以及慢请求头关闭和超大请求头返回 431，检查均未进入 handler。

这不是全链路单一五秒截止保证，各阶段期限独立；也不是所有已建立连接的
总量限制、HTTP/2 压力测试或原始长流无损证明。CLI 尚未开放。远端旧提交
16f9fa1a 的 probe 35328959294 已成功，image 35328959280 仍在运行；该 probe
不包含后续本地增加的 endpoint CI 步骤，不能当作本轮变更的验收。

最终定向三轮 race 通过（59500，31.286 秒）；endpoint、server、
transportidentity、cmd/option 全包单轮 race 分别通过（75380，
54.862/50.686/1.030/1.228 秒），相关 vet 和 diff 检查通过，执行链终态 0。
本地参考 etcd 的 server/embed/serve.go 中普通 HTTP ReadHeaderTimeout
也是五分钟；本轮更严格限制仅用于显式实验 peer，不修改公共 etcd 客户端
端点的兼容性配置。没有集群部署、推送或重跑 CI。

## 显式 CLI 策略入口

增加 `--experimental-peer-retirement-config`，默认空值保持关闭。配置格式、
约束与部署边界见[实验配置说明](experimental_peer_retirement_config_cn.md)。
只读普通文件，最大 64 KiB，允许投影符号链接，非阻塞打开排除 FIFO。
先逐 token 拒绝重复键/过深嵌套和尾随输入，再按精确八字段解析；检查固定
目标、pin、预算与数量限制。文件解析错误不回显输入。三个预算即使使用
相同 duration 字符串也必须独立赋值，不允许缺省或零预算。

参数层要求严格 peer mTLS、本机 identity 存在于 pin 表、远端列表不包含
本机 holder。Validate 和 Run 都读取，Run 在创建 storage client 前重读；
每次先清除旧策略，读失败不回退缓存。运行后策略不热加载，TLS 文件重载
仍为独立机制。后端真实 scope 与本机 TLS key/pin 的最终验证仍由既有
server 构造器执行，不能用文件语法通过代替这些检查。

定向三轮 race（58299）通过：endpoint 1.167 秒、cmd/option 1.208 秒。
包含重复/转义同名键、嵌套重复、未知/大小写错误字段、尾随值、null、
预算越界、明文/带路径 URL、共享 key、未知 holder、超大文件、过深结构、
非法 UTF-8、符号链接、FIFO/目录/设备/缺失文件；参数绑定测试检查默认关闭、
拒绝明文与混合模式、缺失本机 pin、成功加载后文件失效的拒绝，以及绕过
Validate 时 Run 仍在访问 storage 前返回错误。未声称已通过完整二进制部署。

全包单轮 race（52120）通过：cmd/option 1.236 秒、endpoint 53.905 秒、
server 51.261 秒、transportidentity 1.028 秒；相关 vet/diff 通过，执行链
终态 0。收尾时旧提交镜像作业 35328959280 进入 Verify published test image，
仍为 in_progress；未推送、重复触发或部署。用户已有验收状态文档改动保持
未暂存，未将其混入本轮提交。

## CLI 成员 identity 与 HTTPS 固定目标的实际接入修复

新增 TestExperimentalEndpointPairUsesFilePolicyWithCLIIdentities：不再把
HTTPS URL 当作选举 identity，而采用 CLI 的 host:port；两个 Endpoint 的
策略均先写入 JSON，再通过正式文件解析器读取，保留实际 listener、mTLS、
follower Status、Put/leader Get 及 revision 校验。旧实现红测（11722，
19.253 秒）在 follower 可用性等待失败：静态和动态凭据提供器只匹配 URL，
不能匹配正常 CLI 写入选举记录的 host:port。这说明原 URL 身份夹具的成功
不能作为 CLI 已能工作的一致证据。

统一静态/动态凭据目标解析，只接受已配置 HTTPS URL 或其完全相同的 Host
部分，仍绑定该目标的 holder pin；本机仅接受已配置的本机 identity。
不执行 DNS、端口省略或任意 URL 归一化，不把显式 http:// 升级为 HTTPS。
未知地址仍失败，不回退 CA-only 或明文。真实转发使用自定义 TLS credentials，
因此 host:port 形式不是允许明文连接。

新的文件策略双端点 race 三轮通过（95970，28.737 秒）。IPv4/DNS 格式、
IPv6 字面量及本机身份的映射测试同时覆盖静态和动态提供器，拒绝明文 URL、
尾斜线、查询参数、未知目标、前导空格、dns resolver URL；相关三轮 race
通过（48032，1.380 秒）。此夹具仍使用共享 memkv，不是完整 CLI 二进制
或 TiKV 故障验收，且保留既有关闭时等待 successor 的语义限制。

远端旧提交 16f9fa1a 的 image 35328959280 与 probe 35328959294 均已成功。
已核对 image 的构建、推送、发布镜像校验步骤成功；它们不覆盖之后本地
提交，亦未据此部署旧镜像。

完整单轮 race（88211）通过：endpoint 64.363 秒、server 51.148 秒、
etcdproxy 5.458 秒、cmd/option 1.236 秒；相关 vet/diff 通过，执行链终态 0。
另 cmd/build 普通单轮测试通过（41725，0.911/1.500 秒）。旧 CI 已终结，
本轮修复提交后将累计本地提交推送 dbaas，让新 CI 验证包含 endpoint 步骤
的完整新版本；这些本地通过记录不预先证明新 CI 或集群验收成功。
