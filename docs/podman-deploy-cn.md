# 基于 Podman 部署 KubeBrain 集群

> 面向读者：需要用 KubeBrain 替换 etcd、为 Kubernetes 提供可水平扩展存储后端的运维与平台工程师。文中所有 IP、镜像地址均为示例，请按实际环境替换。
>
> **前置条件：PD + TiKV 集群已按《基于 Podman 部署 TiKV + PD 集群》就绪。**

------

## 1. 概述

### 1.1 背景

Kubernetes 默认用 etcd 作为唯一持久化后端。集群规模增长后，etcd 的单 Raft 组、全量数据常驻内存、单实例容量上限（实践中约百万级 key / 8GB）会成为硬瓶颈，且无法水平扩展。

**KubeBrain** 是一个 **etcd v3 协议兼容层（shim）**：它对 kube-apiserver 伪装成一个 etcd 集群，把读写请求翻译到底层的分布式 KV 存储（本文用 TiKV）。apiserver 完全无感知，不需要任何改造。

> 📌 请使用 2026-07-14 之后构建的镜像：自报 etcd 3.7.0、支持 `GET /version`（kubeadm 外部 etcd 预检依赖）、`--advertise-host`、物理 GC 全键空间扫描等关键修复均在此后落地。验证方式：`curl http://<kb>:3379/version` 应返回 `{"etcdserver":"3.7.0",...}`。

### 1.2 本文目的

在若干台专用节点上，用 **Podman + systemd（Quadlet）** 部署 3 副本的 KubeBrain 高可用集群，作为 kube-apiserver 的外部存储（`etcd.external` 形态），并覆盖部署、验证、接入 apiserver、故障切换、监控与排障。

### 1.3 架构：KubeBrain 如何取代 etcd

```
kube-apiserver
  --storage-backend=etcd3
  --etcd-servers=http://kb-1:3379,http://kb-2:3379,http://kb-3:3379
        │
        │  etcd v3 gRPC（apiserver 以为对面是 etcd）
        ▼
  KubeBrain × 3  ── 选主：1 leader + 2 follower ──┐   〔Podman + systemd，无持久状态〕
        │                                          │
        ▼                                          │
   PD × 3  ────  TiKV × 3+                        │   〔已就绪，所有数据在这里〕
                                                   │
        └──────────── 选主锁记录也存在 TiKV ───────┘
```

三个关键特性：

- **KubeBrain 自身无持久状态。** 所有数据（包括选主锁）都落在 TiKV，因此副本可以随意增删、重建，不像 etcd 那样需要 member add/remove。
- **对 apiserver 完全透明。** apiserver 用标准的 `--storage-backend=etcd3 --etcd-servers=...` 直接对接，无需 patch。
- **存储与计算解耦。** 容量与吞吐随 TiKV 横向扩展，突破了 etcd 的单机上限。

### 1.4 高可用模型：选主 + follower 转发

三个副本运行**完全相同**的容器，通过 leader election 协调（锁记录存于 TiKV）：

| 角色                 | 职责                                                         |
| -------------------- | ------------------------------------------------------------ |
| **Leader**（唯一）   | 有序写入、revision 分配、count-index 构建、compaction、**GC safepoint 推进**。所有写最终在 leader 落定。 |
| **Follower**（其余） | 本地直接服务读；写请求自动**转发给 leader**（需开启 `--compatible-with-etcd`）。 |

- **副本身份 = `本机IP:peer-port`**，follower 从锁记录里读到 leader 地址后直接拨过去转发。
- **故障切换**：leader 挂掉 → 租约到期（默认 8s）→ 某个 follower 抢到锁上任。整个过程对 apiserver 透明，它只是感觉某个 etcd 端点短暂不可用。
- **写栅栏防脑裂**：续租超时（默认 5s）时旧 leader 会**主动停止接受写**，避免网络分区下的双写。

> ⚠️ **选主三参数必须满足 `retry < renew < lease`**（默认 1s / 5s / 8s）。改动时务必保持这个不等式，否则会出现频繁误切或脑裂窗口。

### 1.5 KubeBrain 不与 TiKV 同机

KubeBrain leader 承担有序数据收集（collector）工作，对 CPU 和 IO 延迟极其敏感。一旦与 TiKV 同机部署，两者争抢资源会导致 collector 进度爬行，写入频繁触发内部的超时兜底，整体写延迟劣化一个数量级。

**生产部署优先让 KubeBrain 副本与 TiKV 分机**；资源充足（CPU 有余量、NVMe 盘）的场景共置也可行——同栈三机共置(KubeBrain+PD+TiKV)在 1650 万对象/PUT 4700+每秒的实测中表现正常，关键是给 leader 留足 CPU。推荐 6 机拓扑（全部 Podman + systemd，全部先于 k8s 存在）：

| 角色            | IP（示例）                       | 组件                   |
| --------------- | -------------------------------- | ---------------------- |
| 存储节点 1/2/3  | `10.32.32.101` / `.102` / `.103` | PD + TiKV（已部署）    |
| KubeBrain 1/2/3 | `10.32.32.111` / `.112` / `.113` | **KubeBrain 副本 × 3** |

### 1.6 组件与镜像

| 组件          | 镜像                                | 数量 | 角色                                    |
| ------------- | ----------------------------------- | ---- | --------------------------------------- |
| **KubeBrain** | `ghcr.io/fivetime/kubebrain:latest` | 3    | etcd v3 兼容层（1 leader + 2 follower） |

该镜像默认以 TiKV 为后端构建，拉取步骤见 §2.4。

### 1.7 端口规划

KubeBrain 有三个独立平面，各占一个端口：

| 平面       | 本文端口 | 用途                                   |
| ---------- | -------- | -------------------------------------- |
| **client** | `3379`   | 数据面。**apiserver 连这个口。**       |
| **peer**   | `3380`   | follower → leader 写转发、选主身份标识 |
| **info**   | `8080`   | `/metrics`、`/election`，可选 pprof    |

> ⚠️ KubeBrain 的默认端口是 `2379/2380`，与 PD **冲突**。本文显式改用 `3379/3380/8080`（与官方镜像 `EXPOSE` 的端口一致）。若你的 KubeBrain 与 PD 分机部署，理论上不冲突，但仍建议显式区分，避免日后共置时踩坑。

### 1.8 关键约定

- **Podman Quadlet**：`.container` 文件放 `/etc/containers/systemd/`，`daemon-reload` 后自动生成 systemd service，开机自启 + 崩溃自愈。
- **`Network=host`**：KubeBrain 需要拿到宿主机真实 IP 作为选主身份，必须用 host 网络。
- **主线为明文传输**：适用于可信内网。跨不可信网络或有合规要求时启用 TLS，见 **附录 A**。
- **⚠️ GC safepoint 由 KubeBrain leader 负责推进**。裸 TiKV 没有 GC 推进者，如果这条链路断了，MVCC 历史版本会无限堆积、读延迟持续恶化。部署后必须验证（见 §4.4）。

------

## 2. 环境准备

### 2.1 前置条件

| 项目      | 要求                                                 |
| --------- | ---------------------------------------------------- |
| PD + TiKV | 已就绪，`pd-ctl store` 显示所有 Store 为 `Up`        |
| 操作系统  | 支持 systemd 的现代 Linux                            |
| Podman    | **≥ 4.4**（Quadlet 支持的最低版本）                  |
| 节点数    | 3 台（**不能与 TiKV 同机**，见 §1.5）                |
| 网络      | KubeBrain 节点之间 3379/3380 互通；能访问 PD 的 2379 |

### 2.2 安装 Podman（所有 KubeBrain 节点）

```bash
sudo apt-get update && sudo apt-get install -y podman
podman --version

# 确认 Quadlet 生成器存在
ls /usr/lib/systemd/system-generators/podman-system-generator
```

> ⚠️ **Podman 没有官方 apt 源**，直接用发行版自带包即可。老教程里的 openSUSE Kubic 仓库已停止服务。Podman 也**不依赖 containerd 或 CRI-O**——它直接调用 OCI runtime（crun / runc），无常驻守护进程。

### 2.3 目录规划

```bash
sudo mkdir -p /var/log/kubebrain        # 应用日志
sudo mkdir -p /etc/kubebrain            # 集群变量文件
sudo mkdir -p /etc/containers/systemd   # Quadlet 单元文件
# 启用 TLS 时另需 /etc/kubebrain/certs，见附录 A
```

> KubeBrain 无本地数据目录（状态全在 TiKV），因此不存在 TiKV 那样的"日志目录不能是数据目录子目录"的约束。

### 2.4 预拉取镜像

在所有 KubeBrain 节点上执行：

```bash
sudo podman pull ghcr.io/fivetime/kubebrain:latest
```

镜像入口是 `kube-brain` 二进制，Quadlet 的 `Exec=` 只需提供**参数**，会被追加到入口之后。镜像 `EXPOSE` 的端口是 `3379 / 3380 / 8080`，与 §1.7 的端口规划一致。

------

## 3. 集群规划与变量

### 3.1 节点规划（示例）

| 角色           | IP                               | 组件                |
| -------------- | -------------------------------- | ------------------- |
| 存储节点 1/2/3 | `10.32.32.101` / `.102` / `.103` | PD + TiKV（已就绪） |
| KubeBrain 1    | `10.32.32.101`                   | KubeBrain 副本      |
| KubeBrain 2    | `10.32.32.102`                   | KubeBrain 副本      |
| KubeBrain 3    | `10.32.32.103`                   | KubeBrain 副本      |

> 本示例为**共置形态**（KubeBrain 与 PD/TiKV 同机，lab/中小规模足够）；分机形态见 §1.5,只需换 IP,其余配置完全相同。

KubeBrain 副本**没有编号概念**——三台跑完全相同的配置，身份由本机 IP 自动确定，leader 由选举产生。

### 3.2 集群变量

**三台节点内容完全相同**。KubeBrain 副本没有编号、没有 initial-cluster，唯一因机器而异的只有本机 IP：

```bash
export KB_CLUSTER_NAME="prod"
export KB_IMAGE="ghcr.io/fivetime/kubebrain:latest"

# PD 端点（注意：--pd-addrs 不带 scheme）
export PD_ADDRS="10.32.32.101:2379,10.32.32.102:2379,10.32.32.103:2379"

# 本机 IP（自动探测，三台通用）
INTERFACE="k8s-ctl"
NODE_IPV4=$(ip -4 addr show dev $INTERFACE scope global | grep -oP 'inet\s+\K[0-9.]+' | head -n1)
```

------

## 4. 部署

### 4.1 Quadlet 单元模板

```bash
sudo tee /etc/containers/systemd/kubebrain.container.tmpl > /dev/null << 'EOF'
[Unit]
Description=KubeBrain (etcd-v3 shim over PD + TiKV)
After=network-online.target
Wants=network-online.target

[Container]
Image=${KB_IMAGE}
Network=host
Exec=--pd-addrs=${PD_ADDRS} \
     --advertise-host=${NODE_IPV4} \
     --port=3379 \
     --peer-port=3380 \
     --info-port=8080 \
     --compatible-with-etcd=true \
     --cluster-name=${KB_CLUSTER_NAME} \
     --enable-count-index=true \
     --count-index-max-keys=0 \
     --storage-gc-lifetime=10m \
     --enable-storage-metrics=true
Volume=/var/log/kubebrain:/var/log/kubebrain:Z

[Service]
Restart=always
RestartSec=5s
TimeoutStartSec=0
OOMScoreAdjust=-800
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target default.target
EOF
```

> ⚠️ **`[Service]` 段不支持行尾 `#` 注释。** 写 `TimeoutStartSec=0    # 说明` 会让 systemd 把注释当成值的一部分，报 `Failed to parse ...`，该参数被**静默忽略**。注释必须独立成行。（`[Container]` 段的 `Exec=` 用 `\` 续行是允许的。）

> ⚠️ `--pd-addrs` **不带 scheme**（是 `10.32.32.101:2379` 而非 `http://10.32.32.101:2379`），这一点与 `--etcd-servers` 不同，容易写错。

### 4.2 关键参数说明

| 参数                                     | 作用                                                         |
| ---------------------------------------- | ------------------------------------------------------------ |
| `--pd-addrs`                             | 已就绪的 PD 端点，逗号分隔，**不带 scheme**                  |
| `--advertise-host`                       | **副本对外通告的身份 IP。多网卡机器必填**（见 §1.5、§3.3）。留空则自动探测第一张非环回网卡，结果不可控。仅影响通告身份，**监听仍是全网卡** |
| `--port` / `--peer-port` / `--info-port` | 三个平面的端口，见 §1.7                                      |
| `--compatible-with-etcd=true`            | **必开。** 原生 apiserver 依赖它；同时它是 follower → leader 写转发的开关 |
| `--cluster-name`                         | **仅用作监控指标的 `cluster` 标签**（默认 `default`），不参与数据隔离 |
| `--keyspace`                             | **共享存储集群上的租户隔离**(2026-07-15 起,#76):非空时([a-z0-9-],≤64)所有键族(对象/事件日志/内部元数据/协调键)都从它派生独立 magic,不同 keyspace 的集群在同一套 TiKV 上**互相不可见、GC 互不误伤**(如给 Cilium kvstore 单独跑一套 KubeBrain)。空(默认)=原单租户键空间,存量部署零迁移。**同一集群的所有副本必须一致**;对已有数据的集群改 keyspace = 数据"消失"(还在,但在旧租户空间里) |
| `--enable-count-index`                   | 在 leader 上维护内存版本索引，让 List 的 count 免于全表扫描。**依赖 `--compatible-with-etcd`** |
| `--count-index-max-keys`                 | 索引跟踪的 key 数上限，超过则索引自动关闭、回退全扫。默认 `5000000`，**设 `0` = 不限制**（推荐，免去猜总量） |
| `--enable-storage-metrics`               | 开启存储层指标，供 Prometheus 采集                           |
| `--enable-pprof`                         | **仅排障时临时加**。会在 info 端口暴露 pprof，扩大攻击面（永远不会出现在 client 口） |

> ⚠️ **`--key-prefix` 已删除(2026-07-15 起的镜像)**:内部协调键(选主锁、compact 水位)固定使用 `/kubebrain-internal`,与客户端键前缀无关。升级到新镜像时**必须从 `Exec=` 里删掉 `--key-prefix` 行**,否则进程以 `unknown flag` 拒绝启动(故意 fail-loud)。旧镜像(≤2026-07-14)仍需要该参数且必须设 `/registry`(错位=物理 GC 全废,P0)。

### 4.3 部署（三台并行，自动选主）

KubeBrain 副本之间**没有 bootstrap 依赖**——不像 PD 需要预先声明完整成员列表。三台可以任意顺序、任意时刻启动，谁先起来谁先抢锁当 leader。

```bash
envsubst < /etc/containers/systemd/kubebrain.container.tmpl \
  | sudo tee /etc/containers/systemd/kubebrain.container > /dev/null
sudo systemctl daemon-reload
sudo systemctl restart kubebrain.service
```

### 4.4 验证

```bash
KB_NODES="10.32.32.101 10.32.32.102 10.32.32.103"

# 1) 三副本健康
for N in $KB_NODES; do
  echo -n "$N health: "; curl -s "http://$N:3379/health"; echo
done

# 2) 谁是 leader（info 口）
for N in $KB_NODES; do
  echo -n "$N election: "; curl -s "http://$N:8080/election"; echo
done

# 3) 用 etcdctl 打——它就是个 etcd
sudo apt-get install -y etcd-client    # 若未安装
ETCDCTL_API=3 etcdctl --endpoints=http://10.32.32.101:3379 endpoint health
ETCDCTL_API=3 etcdctl --endpoints=http://10.32.32.101:3379 put /smoke/k v1
ETCDCTL_API=3 etcdctl --endpoints=http://10.32.32.102:3379 get /smoke/k   # 经 follower 读，验证转发
ETCDCTL_API=3 etcdctl --endpoints=http://10.32.32.102:3379 del /smoke/k
```

**⚠️ 4) 最重要的一步：确认 GC safepoint 在持续推进**

```bash
# 在任意一台能连到 PD 的机器上执行,间隔 10 分钟以上跑两次
pd-ctl -u http://10.32.32.101:2379 service-gc-safepoint
```

`gc_safe_point` 必须 **> 0 且两次读数之间有增长**——单次 >0 不够!曾实测一个集群 safepoint 冻结 31 小时而值看起来"正常"(leader 异常后未恢复推进);只有"在涨"才是健康。滚动重启 KubeBrain 或 leader 切换后,务必复查一次。

**5) 确认 /version 端点**(kubeadm 外部 etcd 预检 `ExternalEtcdVersion` 依赖):

```bash
curl -s http://10.32.32.101:3379/version    # 应返回 {"etcdserver":"3.7.0","etcdcluster":"3.7.0"}
```

> 📌 **leader-only 指标提醒**:`compact{}`、`count_index_keys`、`storage_gc_safepoint` 只在 **leader** 的 `:8080/metrics` 上出现/推进——在 follower 上查不到它们是正常现象,不是故障。先用 `curl :8080/election` 找到 leader 再看指标。

裸 PD + TiKV 没有 GC 推进者（TiDB 场景由内置 `gc_worker` 负责）。KubeBrain 的 leader 会以保留身份 `gc_worker` 自动推进 safepoint。**如果这里长期为 0，说明 GC 没生效，MVCC 历史版本会无限堆积，表现为读延迟持续恶化、磁盘只增不减。** 这是裸 TiKV 部署最常踩的生产事故，必须在上线前确认。

------

## 5. 接入 kube-apiserver

apiserver 把 KubeBrain 当作一个普通的 etcd 集群：

```yaml
# kube-apiserver 参数节选
- --storage-backend=etcd3
- --etcd-servers=http://10.32.32.101:3379,http://10.32.32.102:3379,http://10.32.32.103:3379
- --etcd-compaction-interval=5m
- --feature-gates=DetectCacheInconsistency=false   # 1.34+ 默认开;大规模下是常驻全量对账扫描税
# --etcd-prefix 保持默认 /registry
# 另建议(static pod env): GOMEMLIMIT 设为节点物理内存的 ~80%,防 OOM→重启→全量重灌螺旋
```

**⚠️ 容量红线(1.36 及更早)**:任何**单一资源类型**的 `对象数 × 平均编码大小` 必须 **< 2GiB**——apiserver 冷启动的 watch-cache 初始化是不分页全量 LIST,响应超过 gRPC int32 上限会**每 40s 重试、永久起不来**(实测 490 万 deployment=2.9GB 即中招;真 etcd 同样中招,协议级限制)。预计单类型超线请上 1.37(EtcdRangeStream 流式分块)。更多生存性配置见 KubeBrain 仓库 `docs/survival-stage0-cn.md`。

要点：

- **三个端点全列。** apiserver 的 gRPC 客户端会自动切到存活副本；连任意一台都可以，读走本地、写自动转发到 leader。
- **⚠️ `--etcd-compaction-interval` 必须配置。** KubeBrain **从不自动 compact**，完全依赖 apiserver 定期驱动。不配置的话历史 revision 会无限增长。
- apiserver 的 `--etcd-prefix` 保持默认 `/registry` 即可,KubeBrain 侧无需(也无法)配置对应项——见 §4.2 后的版本说明。

用 kubeadm 部署时，在 `ClusterConfiguration` 里配 `etcd.external`：

```yaml
etcd:
  external:
    endpoints:
      - http://10.32.32.101:3379
      - http://10.32.32.102:3379
      - http://10.32.32.103:3379
```

------

## 6. 日常运维

### 6.1 升级镜像

```bash
sudo podman pull ghcr.io/fivetime/kubebrain:latest

systemctl restart kubebrain.service
```

> ⚠️ **生产环境请把镜像钉到 digest 或固定 tag**,不要长期跑 `:latest`,更不要给它开 `podman-auto-update`——自动拉新会在不受控的时刻滚动重启副本、触发 leader 漂移(实测踩过:auto-update 半夜滚了 leader)。升级应当是显式、逐副本、可回滚的操作。

### 6.2 查看日志

```bash
journalctl -u kubebrain.service -f

# 过滤 podman 事件噪音，只看 KubeBrain 进程自己打的日志
journalctl -u kubebrain.service --no-pager -n 200 | grep -i 'systemd-kubebrain\['
```

重点关注：**选主切换**、**写入超时兜底**（说明 leader 在爬行，多半是与 TiKV 抢资源）、**CAS 冲突率**。

### 6.3 生命周期与配置变更

统一由 systemd 管理，**不要**用 `podman stop/restart` 直接操作容器。

```bash
编辑 .container.tmpl
      ↓
source /etc/kubebrain/cluster.env
      ↓
envsubst 重新渲染 .container
      ↓
systemctl daemon-reload
      ↓
systemctl restart kubebrain.service      # ⚠️ 必须 restart，start 不会重载配置
```

生产环境请**逐副本滚动重启**，不要三台一起重启。

### 6.4 副本增减

KubeBrain 无持久状态，增删副本极其简单——**没有 etcd 那样的 member add/remove 操作**：

```bash
# 扩容：在新机器上按 §2 ~ §4 部署，启动即自动参与选主
# 缩容：
sudo systemctl stop kubebrain.service
sudo rm /etc/containers/systemd/kubebrain.container
sudo systemctl daemon-reload
```

> ⚠️ 缩容后记得同步更新 apiserver 的 `--etcd-servers`，否则它会一直重试一个已经不存在的端点。

------

## 7. 高可用与故障切换

**故障切换是自动的，无需人工介入。** leader 挂掉后租约到期（默认 8s），某个 follower 抢到锁上任；systemd 的 `Restart=always` 会重建崩溃的容器，重建后它以 follower 身份重新加入。

对 apiserver 的影响：`--etcd-servers` 列了三个端点，gRPC 客户端会自动切到存活副本，informer 不会断流。

**演练：**

```bash
# 在 leader 机器上
sudo systemctl restart kubebrain.service

# 在另一台观察 leader 变更
watch -n1 'curl -s http://10.32.32.102:8080/election'
```

**选主参数调优**（更快切换 vs 更少误切）：

```
--leader-retry-period=1s     # 抢锁重试间隔
--leader-renew-deadline=5s   # 续租截止；同时是旧 leader 的自我隔离界（写栅栏）
--leader-lease-duration=8s   # 租约时长；到期后其他副本可抢锁
```

> ⚠️ **必须保持 `retry < renew < lease`。** 默认值 1s / 5s / 8s 已经是实战平衡点，除非有明确理由否则不要改。调小 lease 会加快切换但增加网络抖动下的误切风险。

------

## 8. 监控接入

KubeBrain 不是 k8s Pod，用**监控外部 etcd 的同款方式**接入 Prometheus：headless Service + 手工维护的 Endpoints，指向副本的 **info 端口 `:8080/metrics`**。

```bash
cat << 'EOF' | kubectl apply -f -
apiVersion: v1
kind: Service
metadata: {name: kubebrain, namespace: monitoring, labels: {app: kubebrain}}
spec: {clusterIP: None, ports: [{name: metrics, port: 8080}]}
---
apiVersion: v1
kind: Endpoints
metadata: {name: kubebrain, namespace: monitoring, labels: {app: kubebrain}}
subsets:
- addresses: [{ip: 10.32.32.101}, {ip: 10.32.32.102}, {ip: 10.32.32.103}]
  ports: [{name: metrics, port: 8080}]
---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata: {name: kubebrain, namespace: monitoring}
spec:
  namespaceSelector: {matchNames: [monitoring]}
  selector: {matchLabels: {app: kubebrain}}
  endpoints: [{port: metrics, interval: 30s}]
EOF
```

**核心告警项：**

| 指标             | 关注点                                         |
| ---------------- | ---------------------------------------------- |
| leader 身份      | 频繁切换 = 网络抖动或租约参数不合理            |
| revision 水位    | 只增不减 = compaction 没被 apiserver 驱动      |
| **`compact{}` 行计数**(leader-only) | **有写入/删除却长期冻结 = 物理 GC 停摆**(旧版前缀错位的典型症状),MVCC 垃圾在堆积 |
| `count_index_keys`(leader-only) | 应 ≈ 集群真实对象数;恒 0 = count index 没建起来,大规模 List count 会退化全扫 |
| CAS 冲突率       | 突增 = 写热点或 leader 爬行                    |
| 写入超时兜底次数 | 非零 = leader 资源不足，检查是否与 TiKV 同机   |
| **GC safepoint** | **必须 > 0 且持续增长**（在 PD 侧查，见 §4.4;leader 切换后复查） |

> 上述 leader-only 指标做告警时注意:要么按 `/election` 动态定位 leader 抓取,要么对三副本取 max 再判断,直接对单一固定节点告警会误报。

> ⚠️ 存储层先于 k8s 存在，监控也应当先于 k8s 存在。如果 k8s 集群尚未部署，建议先在存储机上用同样的 Podman + Quadlet 方式跑一套 Prometheus + Grafana，不要等到 k8s 起来才有可观测性。

------

## 9. 规模与调优

| 关注点                           | 处置                                                         |
| -------------------------------- | ------------------------------------------------------------ |
| **KubeBrain leader ≠ TiKV 同机** | **拓扑铁律**，见 §1.5。违反此条会导致写延迟劣化一个数量级    |
| **GC safepoint**                 | leader 自动推进，但**必须验证** `gc_safe_point > 0`          |
| **compaction**                   | 由 apiserver 的 `--etcd-compaction-interval` 驱动，KubeBrain 自己**从不 compact** |
| **大规模 List/count**            | 开 `--enable-count-index`，且 `--count-index-max-keys ≥ 预期总 key 数` |
| **watch 重连风暴**               | 调 `--watch-history-scan-rev-bucket`，让邻近 revision 的历史扫描合并成一次 |
| **watch 缓存**                   | `--watch-cache-size` 按活跃 watcher × 事件速率调大           |
| **CPU 供给**                     | KubeBrain leader 的 collector 吃 CPU，机器要留足余量；TiKV 侧则调 grpc / apply / store 线程池 |

------

## 10. 故障排查

### 10.1 起不来

```bash
journalctl -u kubebrain.service --no-pager -n 200 | grep -i 'systemd-kubebrain\['
sudo podman logs systemd-kubebrain 2>&1 | tail -50
```

| 现象 / 错误信息                               | 原因与处理                                                   |
| --------------------------------------------- | ------------------------------------------------------------ |
| `local ip is empty`                           | 拿不到宿主机 IP。确认 `Network=host` 且本机有非环回 IP       |
| 连不上 PD                                     | `--pd-addrs` 不可达，或**误加了 `http://` scheme**（该参数不带 scheme）。用 `pd-ctl -u http://10.32.32.101:2379 store` 自测 PD 是否健康 |
| 渲染出的 `.container` 有空值（`--pd-addrs=`） | `envsubst` 时变量未生效。确认已 `source /etc/kubebrain/cluster.env`，不能用 `bash` 执行 |
| systemd 报 `Failed to parse ...`              | `[Service]` 段写了行尾 `#` 注释。注释必须独立成行（见 §4.1） |
| 改了配置但行为没变                            | 用了 `systemctl start`（对 running 服务是 no-op）。改用 `restart` |
| systemd 里根本没有该 service                  | 确认文件名是 `*.container`（不是 `.tmpl`），且已 `daemon-reload` |
| 无 leader / 反复切换                          | 检查 TiKV 是否 write-stall（盘满），以及租约参数是否满足 `retry < renew < lease` |

**渲染结果自检：**

```bash
grep -E 'pd-addrs|port=' /etc/containers/systemd/kubebrain.container
ss -lntp | grep -E '3379|3380|8080'
```

### 10.2 读延迟持续恶化

**第一件事：查 GC safepoint。**

```bash
pd-ctl -u http://10.32.32.101:2379 service-gc-safepoint    # 必须 > 0 且在推进
```

长期为 0 说明 MVCC 历史版本在无限堆积。排查 KubeBrain leader 是否正常、GC 推进链路是否断了。

### 10.3 写延迟高、频繁超时兜底

**几乎可以肯定是 leader 与 TiKV 抢资源。** 检查拓扑，把 KubeBrain 迁到独立机器（§1.5）。

### 10.4 大规模 List / count 超时

count-index 没开，或 `--count-index-max-keys` 设得比实际 key 数小，导致回退到全表扫描。

```bash
journalctl -u kubebrain.service | grep -i 'count.*index\|full scan'
```

### 10.5 apiserver informer 断流

多见于中间有 LB / 防火墙杀掉空闲 gRPC 连接。KubeBrain 的 gRPC keepalive 已与 etcd 对齐，检查中间设备的空闲超时配置。

------

## 附录 A. 启用 TLS（可选）

明文足以覆盖可信内网。跨不可信网络、多租户或有合规要求时启用。

**KubeBrain 三个平面各有独立的 TLS 配置**，可以分别启用：

### A.1 KubeBrain 侧（在 `Exec=` 中追加）

```ini
# client 平面（apiserver 连它）
     --cert-file=/etc/kubebrain/certs/kb.crt \
     --key-file=/etc/kubebrain/certs/kb.key \
     --trusted-ca-file=/etc/kubebrain/certs/ca.crt \
     --client-cert-auth=true \
# peer 平面（follower ↔ leader 转发 + 选主）：独立一套
     --peer-cert-file=/etc/kubebrain/certs/kb.crt \
     --peer-key-file=/etc/kubebrain/certs/kb.key \
     --peer-trusted-ca-file=/etc/kubebrain/certs/ca.crt \
     --peer-client-cert-auth=true \
# info 平面（可选给 /metrics 加 TLS，默认明文）
#    --info-cert-file=... --info-key-file=... --info-trusted-ca-file=...
```

并在 `.container` 中挂载证书卷：

```ini
Volume=/etc/kubebrain/certs:/etc/kubebrain/certs:ro
```

**证书要求**：SAN 覆盖所有 KubeBrain 节点 IP + `127.0.0.1`；由于 peer 平面上副本互为客户端与服务端，`extendedKeyUsage` 必须同时包含 `serverAuth` 和 `clientAuth`。

### A.2 apiserver 侧

```yaml
- --etcd-servers=https://10.32.32.101:3379,https://10.32.32.102:3379,https://10.32.32.103:3379
- --etcd-cafile=/etc/kubebrain/certs/ca.crt
- --etcd-certfile=/etc/kubebrain/certs/apiserver-client.crt
- --etcd-keyfile=/etc/kubebrain/certs/apiserver-client.key
```

### A.3 KubeBrain → TiKV

若 PD / TiKV 也启用了 TLS，KubeBrain 需要相应的 CA 与客户端证书配置，并把 `--pd-addrs` 对应的连接切到 TLS 模式。

------

## 附录 B. 参数速查

| 类别    | 参数                                    | 默认    | 说明                                            |
| ------- | --------------------------------------- | ------- | ----------------------------------------------- |
| 存储    | `--pd-addrs`                            | —       | PD 端点，逗号分隔，**不带 scheme**              |
| 端口    | `--port`                                | 2379    | client 平面（apiserver 入口）。本文改为 3379    |
| 端口    | `--peer-port`                           | 2380    | peer 平面（转发 + 选主身份）。本文改为 3380     |
| 端口    | `--info-port`                           | —       | `/metrics`、`/election`。本文用 8080            |
| 兼容    | `--compatible-with-etcd`                | false   | **必开**。原生 apiserver 依赖；同时是写转发开关 |
| GC      | `--storage-gc-lifetime`                 | 10m     | leader 推进 TiKV GC safepoint 的保留窗口。**0=关闭;裸 PD+TiKV 必须开**(无 TiDB 时它是唯一推进者) |
| 命名    | `--cluster-name`                        | —       | 同一 TiKV 上多集群隔离                          |
| 选主    | `--leader-lease-duration`               | 8s      | 租约时长                                        |
| 选主    | `--leader-renew-deadline`               | 5s      | 续租截止 = 写栅栏自我隔离界                     |
| 选主    | `--leader-retry-period`                 | 1s      | 抢锁重试间隔                                    |
| 索引    | `--enable-count-index`                  | false   | 大规模 count 免全扫                             |
| 索引    | `--count-index-max-keys`                | 5000000 | 需 ≥ 预期总 key 数;**0 = 不限制(推荐)**       |
| compact | `--auto-compaction-retention-revisions` | 0（关） | 仅作安全网(如 50000000)，主力靠 apiserver 驱动 |
| watch   | `--watch-cache-size`                    | —       | 按 watcher × 事件率调大                         |
| watch   | `--watch-history-scan-rev-bucket`       | —       | 重连风暴时合并历史扫描                          |
| watch   | `--watch-progress-notify-interval`      | 1s      | progress 通知间隔。**必须 < 2.5s**(apiserver 一致读只等 3s,超限会被 KubeBrain 启动校验直接拒绝) |
| 调试    | `--enable-pprof`                        | false   | **仅临时开启**，会扩大攻击面                    |

**副本身份** = `本机IP:peer-port`，由 `Network=host` 下自动探测的宿主机 IP 决定。

------

## 附录 C. 参考资料

- KubeBrain 上游项目：https://github.com/kubewharf/kubebrain
- 本文使用的镜像：`ghcr.io/fivetime/kubebrain:latest`
- TiKV 官方文档：https://tikv.org/docs/
- Podman Quadlet：`man podman-systemd.unit`
- kubeadm 外部 etcd 配置：Kubernetes 官方文档中的 "Set up a High Availability etcd cluster with kubeadm"
- 配套文档：《基于 Podman 部署 TiKV + PD 集群》
