#!/usr/bin/env bash
# Disaster-recovery regression for the leader-maintained count index.
#
# Drives sustained CREATE-only writes while repeatedly force-killing the current
# leader, and asserts the count index survives every failover correctly:
#   - a rev=0 CountOnly served by the leader's in-memory index (fast, ~ms) must
#     return the SAME count as a full storage scan (slow, served by a follower or
#     a not-yet-ready leader) — index and scan must agree at all times;
#   - no read ever returns a grossly wrong (near-0 / partial) count while the
#     index is rebuilding after a kill (guards the Reset install-gap / partial-
#     load-served / Ready-Count TOCTOU classes of bug);
#   - once writes quiesce and the index rebuilds, every read returns an identical
#     count regardless of which pod/mechanism served it;
#   - no kubebrain pod crash-loops (surviving replicas keep 0 restarts).
#
# The leader is identified by the count_index_keys gauge (leader-only: followers
# never rebuild their index, so their gauge stays 0 and their reads fall back to
# a correct scan). Reads dial a fresh connection each time so the NodePort
# load-balances them across pods — a persistent gRPC conn would pin to one pod
# and never exercise the leader's index. Requires --enable-count-index=true.
#
# Env knobs (all optional):
#   KUBE_CONTEXT NAMESPACE DEPLOYMENT LABEL METRICS_PORT ENDPOINT CLIENT_PORT
#   KILLS WRITE_SECONDS QUIESCE_SECONDS WORKERS
set -euo pipefail

NAMESPACE="${NAMESPACE:-kubebrain-dev}"
DEPLOYMENT="${DEPLOYMENT:-kubebrain}"
LABEL="${LABEL:-app.kubernetes.io/name=kubebrain}"
SVC="${SVC:-kubebrain}"
METRICS_PORT="${METRICS_PORT:-8080}"
CLIENT_PORT="${CLIENT_PORT:-3379}"
KILLS="${KILLS:-6}"
WRITE_SECONDS="${WRITE_SECONDS:-90}"
QUIESCE_SECONDS="${QUIESCE_SECONDS:-30}"
WORKERS="${WORKERS:-48}"

KUBECTL=(kubectl)
if [ -n "${KUBE_CONTEXT:-}" ]; then
  KUBECTL+=(--context "$KUBE_CONTEXT")
fi

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}
need kubectl
need go

pods() {
  "${KUBECTL[@]}" -n "$NAMESPACE" get pods -l "$LABEL" \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'
}

gauge() { # $1=pod -> count_index_keys (0 if absent)
  local v
  v="$("${KUBECTL[@]}" -n "$NAMESPACE" exec "$1" -- wget -qO- "localhost:${METRICS_PORT}/metrics" 2>/dev/null \
        | awk -F'[ ]' '/^count_index_keys\{/{print $2}')"
  printf '%s' "${v%.*}"
}

leader() { # -> "pod keys" (highest count_index_keys; empty if none > 0)
  local best="" bestv=-1 p v
  for p in $(pods); do
    v="$(gauge "$p")"; v="${v:-0}"
    if [ "$v" -gt "$bestv" ] 2>/dev/null; then bestv="$v"; best="$p"; fi
  done
  printf '%s %s' "$best" "$bestv"
}

restart_total() {
  "${KUBECTL[@]}" -n "$NAMESPACE" get pods -l "$LABEL" \
    -o jsonpath='{range .items[*]}{.status.containerStatuses[0].restartCount}{"\n"}{end}' \
    | awk '{s+=$1} END{print s+0}'
}

# ---- resolve a load-balancing endpoint (NodePort) unless ENDPOINT is given ----
if [ -z "${ENDPOINT:-}" ]; then
  node_ip="$("${KUBECTL[@]}" get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')"
  node_port="$("${KUBECTL[@]}" -n "$NAMESPACE" get svc "$SVC" \
    -o jsonpath="{.spec.ports[?(@.port==${CLIENT_PORT})].nodePort}")"
  if [ -z "$node_ip" ] || [ -z "$node_port" ]; then
    echo "could not resolve a NodePort endpoint (node_ip=${node_ip} node_port=${node_port});" >&2
    echo "set ENDPOINT to a load-balancing endpoint that reaches all replicas." >&2
    exit 1
  fi
  ENDPOINT="${node_ip}:${node_port}"
fi
echo "endpoint=${ENDPOINT} kills=${KILLS} write=${WRITE_SECONDS}s quiesce=${QUIESCE_SECONDS}s workers=${WORKERS}"

# sanity: count index must be enabled (some pod exposes the gauge). Capture the
# metrics to a var and glob-match it — a `wget | grep -q` pipe would SIGPIPE wget
# and, under pipefail, report failure even on a match.
have_gauge=0
for p in $(pods); do
  metrics="$("${KUBECTL[@]}" -n "$NAMESPACE" exec "$p" -- wget -qO- "localhost:${METRICS_PORT}/metrics" 2>/dev/null || true)"
  if [[ "$metrics" == *count_index_keys\{* ]]; then have_gauge=1; break; fi
done
if [ "$have_gauge" -ne 1 ]; then
  echo "count_index_keys gauge not exposed — is --enable-count-index=true set?" >&2
  exit 1
fi

restarts_before="$(restart_total)"

# ---- generate the self-contained writer+reader harness ----
workdir="$(mktemp -d)"
cleanup() { rm -rf "$workdir"; }
trap cleanup EXIT
cd "$workdir"
go mod init kubebrain-countindex-failover >/dev/null
go get go.etcd.io/etcd/client/v3@v3.5.2 >/dev/null 2>&1

cat > main.go <<'GOEOF'
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func envi(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil && v > 0 {
		return v
	}
	return def
}

func main() {
	ep := os.Getenv("ENDPOINT")
	prefix := os.Getenv("PREFIX")
	workers := envi("WORKERS", 48)
	writeSecs := envi("WRITE_SECONDS", 90)
	quiesceSecs := envi("QUIESCE_SECONDS", 30)

	// countOnce dials fresh so the NodePort re-picks a backend each read.
	countOnce := func(timeout time.Duration) (int64, time.Duration, error) {
		rc, e := clientv3.New(clientv3.Config{Endpoints: []string{ep}, DialTimeout: 5 * time.Second})
		if e != nil {
			return 0, 0, e
		}
		defer rc.Close()
		ctx, cc := context.WithTimeout(context.Background(), timeout)
		defer cc()
		t := time.Now()
		r, err := rc.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
		lat := time.Since(t)
		if err != nil {
			return 0, lat, err
		}
		return r.Count, lat, nil
	}

	wcli, err := clientv3.New(clientv3.Config{Endpoints: []string{ep}, DialTimeout: 5 * time.Second})
	if err != nil {
		fmt.Println("dial:", err)
		os.Exit(2)
	}
	defer wcli.Close()

	var confirmed, nextKey, runningMax int64
	t0 := time.Now()
	stopWrite := make(chan struct{})
	stopRead := make(chan struct{})

	// writers: CREATE-only so the live count is monotonic during the write phase.
	var wwg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wwg.Add(1)
		go func() {
			defer wwg.Done()
			for {
				select {
				case <-stopWrite:
					return
				default:
				}
				id := atomic.AddInt64(&nextKey, 1) - 1
				ctx, cc := context.WithTimeout(context.Background(), 10*time.Second)
				_, e := wcli.Put(ctx, fmt.Sprintf("%sk%08d", prefix, id), "payload-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
				cc()
				if e == nil {
					atomic.AddInt64(&confirmed, 1)
				}
			}
		}()
	}

	// reader: rev=0 CountOnly; flag a fast (index) read that is grossly below the
	// running max seen (regression far beyond write-lag => Reset gap / partial load).
	grossUnder := int64(0)
	go func() {
		tick := time.NewTicker(250 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stopRead:
				return
			case <-tick.C:
			}
			count, lat, e := countOnce(20 * time.Second)
			if e != nil {
				continue
			}
			m := atomic.LoadInt64(&runningMax)
			if count > m {
				atomic.StoreInt64(&runningMax, count)
				m = count
			}
			served := lat < 80*time.Millisecond
			if served && m > 500 && count < m/2 {
				n := atomic.AddInt64(&grossUnder, 1)
				fmt.Printf("[%s] GROSS-UNDERCOUNT fast-read count=%d runningMax=%d (#%d)\n",
					time.Since(t0).Round(time.Millisecond), count, m, n)
			}
		}
	}()

	go func() {
		last := int64(0)
		for {
			time.Sleep(10 * time.Second)
			select {
			case <-stopRead:
				return
			default:
			}
			c := atomic.LoadInt64(&confirmed)
			fmt.Printf("[%4ds] confirmed=%-9d (+%d/10s) runningMax=%d\n",
				int(time.Since(t0).Seconds()), c, c-last, atomic.LoadInt64(&runningMax))
			last = c
		}
	}()

	time.Sleep(time.Duration(writeSecs) * time.Second)
	close(stopWrite)
	wwg.Wait()
	fmt.Printf(">>> writes stopped at %s, confirmed=%d — quiescing %ds for index to settle...\n",
		time.Since(t0).Round(time.Second), atomic.LoadInt64(&confirmed), quiesceSecs)
	time.Sleep(time.Duration(quiesceSecs) * time.Second)
	close(stopRead)

	// ---- correctness oracle: at quiesce, index and scan must AGREE ----
	// 18 fresh-conn reads; collect distinct counts and whether the index served
	// any of them. All counts must be identical; the index must have served >=1.
	fmt.Println("=== FINAL: index-vs-scan agreement (all reads must return the same count) ===")
	counts := map[int64]int{}
	servedByIndex, scanReads, errs := 0, 0, 0
	var anyCount int64 = -1
	for i := 0; i < 18; i++ {
		count, lat, e := countOnce(20 * time.Second)
		if e != nil {
			errs++
			fmt.Printf("  #%2d ERR %v\n", i, e)
			continue
		}
		tag := "scan "
		if lat < 80*time.Millisecond {
			tag = "INDEX"
			servedByIndex++
		} else {
			scanReads++
		}
		counts[count]++
		anyCount = count
		fmt.Printf("  #%2d %-5s %7s count=%d\n", i, tag, lat.Round(time.Millisecond), count)
		time.Sleep(300 * time.Millisecond)
	}

	fmt.Println("================ RESULT ================")
	fmt.Printf("confirmed(client-acked, exact unique-key count)=%d runningMax=%d\n",
		atomic.LoadInt64(&confirmed), atomic.LoadInt64(&runningMax))
	fmt.Printf("final reads: index=%d scan=%d err=%d distinctCounts=%d gross-undercounts=%d\n",
		servedByIndex, scanReads, errs, len(counts), atomic.LoadInt64(&grossUnder))

	fail := false
	if atomic.LoadInt64(&grossUnder) > 0 {
		fmt.Println("FAIL: a fast (index) read returned a grossly wrong count during rebuild")
		fail = true
	}
	if len(counts) > 1 {
		fmt.Printf("FAIL: index and scan disagree at quiesce, counts=%v\n", counts)
		fail = true
	}
	if expected := atomic.LoadInt64(&confirmed); anyCount != expected {
		fmt.Printf("FAIL: settled count=%d differs from %d acknowledged unique-key puts\n", anyCount, expected)
		fail = true
	}
	if servedByIndex == 0 {
		fmt.Println("FAIL: the index never served a read (endpoint did not reach the leader?)")
		fail = true
	}
	if scanReads == 0 {
		fmt.Println("WARN: no scan read to cross-check against (all reads hit the index)")
	}
	// Cleanup our keys below the production DeleteRange cap. Cleanup failures are
	// test failures: leaking this prefix corrupts later count-index baselines.
	hi := atomic.LoadInt64(&nextKey)
	del := int64(0)
	cleanupErrs := 0
	for lo := int64(0); lo < hi+1; lo += 500 {
		ctx, cc := context.WithTimeout(context.Background(), 60*time.Second)
		r, e := wcli.Delete(ctx,
			fmt.Sprintf("%sk%08d", prefix, lo),
			clientv3.WithRange(fmt.Sprintf("%sk%08d", prefix, lo+500)))
		cc()
		if e != nil {
			cleanupErrs++
			fmt.Printf("cleanup range [%d,%d) failed: %v\n", lo, lo+500, e)
			continue
		}
		del += r.Deleted
	}
	left, _, cleanupCountErr := countOnce(20 * time.Second)
	if cleanupCountErr != nil || cleanupErrs != 0 || left != 0 {
		fmt.Printf("FAIL: cleanup deleted=%d rangeErrors=%d left=%d countErr=%v under %s\n",
			del, cleanupErrs, left, cleanupCountErr, prefix)
		fail = true
	} else {
		fmt.Printf("cleanup: deleted=%d left=0 keys under %s\n", del, prefix)
	}

	if fail {
		fmt.Printf("RESULT: FAIL (settled count=%d)\n", anyCount)
		os.Exit(1)
	}
	if scanReads == 0 {
		fmt.Printf("RESULT: PASS — index==acknowledged unique puts==%d across %d kills; no scan sample\n",
			anyCount, envi("KILLS", 0))
	} else {
		fmt.Printf("RESULT: PASS — index==scan==acknowledged unique puts==%d across %d kills\n",
			anyCount, envi("KILLS", 0))
	}
}
GOEOF

# ---- run harness in background, kill the leader KILLS times across the write phase ----
prefix="/registry/countindex-failover-$(date +%s)/"
log="$workdir/harness.log"
ENDPOINT="$ENDPOINT" PREFIX="$prefix" WORKERS="$WORKERS" \
  WRITE_SECONDS="$WRITE_SECONDS" QUIESCE_SECONDS="$QUIESCE_SECONDS" KILLS="$KILLS" \
  go run main.go >"$log" 2>&1 &
hpid=$!

# space kills across the write phase (first at ~15s, last near the end)
sleep 15
gap=$(( (WRITE_SECONDS - 15) / (KILLS > 1 ? KILLS - 1 : 1) ))
[ "$gap" -lt 5 ] && gap=5
kn=0
while [ "$kn" -lt "$KILLS" ]; do
  read -r pod keys <<<"$(leader)"
  kn=$((kn + 1))
  if [ -n "$pod" ]; then
    echo "[kill #${kn}/${KILLS} $(date +%T)] leader=${pod} count_index_keys=${keys} -> force delete"
    "${KUBECTL[@]}" -n "$NAMESPACE" delete pod "$pod" --force --grace-period=0 >/dev/null 2>&1 || true
  else
    echo "[kill #${kn}/${KILLS}] no leader gauge found; skipping"
  fi
  [ "$kn" -lt "$KILLS" ] && sleep "$gap"
done

echo "=== waiting for harness (quiesce + final agreement check) ==="
harness_rc=0
wait "$hpid" || harness_rc=$?
# drop the etcd client library's retry-warning noise (expected during kills);
# the harness's own output is plain text and is preserved.
grep -v '"logger":"etcd-client"' "$log" || true

# ---- post: pod health + verdict ----
restarts_after="$(restart_total)"
echo "pod restarts: before=${restarts_before} after=${restarts_after}"
"${KUBECTL[@]}" -n "$NAMESPACE" get pods -l "$LABEL"

if [ "$harness_rc" -ne 0 ]; then
  echo "countindex-failover-smoke: FAIL (harness rc=${harness_rc})" >&2
  exit 1
fi
if [ "$restarts_after" -gt "$restarts_before" ]; then
  echo "countindex-failover-smoke: FAIL — a pod crash-looped (restarts ${restarts_before}->${restarts_after})" >&2
  exit 1
fi
echo "countindex-failover-smoke: PASS"
