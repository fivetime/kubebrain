# Kubernetes 上游 PR 草稿(fivetime/kubernetes)

两个分支已推送到 fork,基于 upstream/master(1a92e1c4533)。
开 PR:https://github.com/kubernetes/kubernetes/compare/master...fivetime:kubernetes:<branch>
需先签 CNCF CLA(https://git.k8s.io/community/CLA.md)。commit author 已设为 `fivetime <jp.zdm2008@gmail.com>`,如需真实姓名可 `git commit --amend --author` 后 force-push。

---

## PR 1(优先提交,bug fix):`upstream-pr/repair-initial-retry`

**Title**: `Retry services repair initial sync with backoff instead of waiting a full interval`

**Body**:

```
**What type of PR is this?**
/kind bug
/sig network

**What this PR does / why we need it:**

The ClusterIP/NodePort repair loops treat the FIRST successful sync
specially: the apiserver's post-start hook blocks readiness on it. But
when that first runOnce fails transiently (storage not yet warm right
after startup, a blip, etc.):

- `repair.go` (ClusterIP/NodePort): RunUntil waits a full
  `interval` (3 minutes) before the next attempt, while the post-start
  hook only waits 1 minute — so one transient failure at startup
  guarantees the hook deadline is exceeded and the apiserver exits,
  even though a retry two seconds later would have succeeded.

- `repairip.go` (MultiCIDRServiceAllocator): worse — a first-sync
  failure returns from the goroutine permanently; the repair loop never
  runs for the lifetime of the process.

This PR retries the initial sync with exponential backoff
(1s, 2s, 4s, ... capped at the repair interval) until it succeeds,
in both implementations, and fixes the permanent-give-up path in
repairip.go.

**Which issue(s) this PR fixes:**
None filed; found while operating clusters where the first List after
apiserver restart can transiently fail.

**Special notes for your reviewer:**
The regression test makes the first two Service Lists fail via a
reactor and asserts the first successful sync completes within seconds
instead of a full interval (it previously took 3 minutes).

**Does this PR introduce a user-facing change?**
```release-note
The ClusterIP/NodePort repair controllers now retry a failed initial
sync with exponential backoff instead of waiting a full repair
interval, and the MultiCIDRServiceAllocator repair loop no longer
permanently stops after a failed initial sync.
```
```

---

## PR 2(需 sig-network 讨论,规模化):`upstream-pr/repair-scale-tunables`

**Title**: `Scale the Services repair loops for very large Service counts`

**Body**:

```
**What type of PR is this?**
/kind feature
/sig network
/sig scalability

**What this PR does / why we need it:**

The Services repair loops have two hard-coded timings that break down
at very large Service counts, where a single runOnce pass — a full
quorum List of every Service — takes minutes:

1. The post-start hooks wait at most 1 minute for the first successful
   pass and kill the apiserver otherwise. With ~1.16M Services the
   first pass alone exceeds the deadline, so the apiserver crash-loops
   on every restart. This PR raises the deadline to 15 minutes; the
   hook still fails fast on errors — this only extends how long a
   *running* first pass may take.

2. The repair cadence is 3 minutes. Each pass Lists every Service from
   quorum storage, so at millions of Services a full-keyspace scan is
   almost permanently in flight, measurably crowding out write traffic
   (in our measurements repair scans were half of all storage scan
   load on a 33M-key cluster). The loops are a safety net for leaked
   allocations, not a correctness-critical path; this PR stretches the
   cadence to 30 minutes.

Both values were validated on a 33M-key cluster (1.16M Services,
100k Nodes, 8.8M Pods, etcd-compatible storage backend).

**Special notes for your reviewer:**
If changing the defaults is undesirable, I am happy to rework either
value into a configuration knob (e.g. wired through
`Extra.RepairServicesInterval`, which already exists on the config
struct) — guidance welcome on the preferred shape.

**Does this PR introduce a user-facing change?**
```release-note
The Services repair loops now allow up to 15 minutes for their first
pass at apiserver startup (was 1 minute) and run every 30 minutes
(was 3 minutes), accommodating clusters with very large Service
counts.
```
```

---

## 提交前 checklist

- [ ] CNCF CLA 已签(GitHub 账号关联 jp.zdm2008@gmail.com)
- [ ] 如需真实姓名:两分支 `git commit --amend --author="Your Name <jp.zdm2008@gmail.com>"` + force-push
- [ ] PR 1 先提;PR 2 在描述里主动提供 flag 化备选,等 sig-network 反馈
- [ ] 本地已验证:PR 1 回归测试绿(portallocator/ipallocator controller);PR 2 编译绿
```
