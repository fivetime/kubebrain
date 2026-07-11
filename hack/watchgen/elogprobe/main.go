// elogprobe validates the event-log replay path (#45/#52) against a live
// cluster: write far more events than the leader's ring holds, reconnect a
// watch below the ring's oldest revision, and assert the replayed stream is
// complete (every sequence exactly once, revisions strictly increasing) and
// PrevKv-correct (updates carry the previous value; creates carry none).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	endpoint := flag.String("endpoint", "10.224.0.14:3379", "etcd endpoint (the KubeBrain LEADER)")
	prewrites := flag.Int("prewrites", 300, "writes before the watch anchor rolls out of the ring")
	postwrites := flag.Int("postwrites", 6000, "writes after the anchor; must exceed the ring size")
	hotkeys := flag.Int("hotkeys", 20, "hot keys receiving repeated updates (PrevKv checks)")
	flag.Parse()

	ctx := context.Background()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{*endpoint}, DialTimeout: 5 * time.Second})
	must(err)
	defer cli.Close()

	prefix := fmt.Sprintf("/elogprobe/%d/", time.Now().Unix())
	fmt.Printf("prefix=%s\n", prefix)

	// lastVal tracks, per key, the value the NEXT event's PrevKv must carry.
	lastVal := map[string]string{}
	seq := 0
	write := func(n int) (lastRev int64) {
		for i := 0; i < n; i++ {
			var key string
			if seq%3 == 0 { // one third hot-key updates → PrevKv assertions
				key = fmt.Sprintf("%shot-%03d", prefix, seq%*hotkeys)
			} else {
				key = fmt.Sprintf("%scold-%07d", prefix, seq)
			}
			val := fmt.Sprintf("s%07d", seq)
			resp, err := cli.Put(ctx, key, val)
			must(err)
			lastRev = resp.Header.Revision
			lastVal[key] = val
			seq++
		}
		return lastRev
	}

	// Anchor: current revision before any probe writes.
	g, err := cli.Get(ctx, prefix, clientv3.WithCountOnly())
	must(err)
	anchorRev := g.Header.Revision
	expectPrev := map[string]string{} // key -> value at anchor time (empty map: all creates)

	fmt.Printf("anchor=%d, writing %d pre + %d post (ring must be smaller than the total)...\n",
		anchorRev, *prewrites, *postwrites)
	t0 := time.Now()
	write(*prewrites)
	endRev := write(*postwrites)
	total := *prewrites + *postwrites
	fmt.Printf("wrote %d events in %s (last rev %d); reconnecting watch at anchor+1=%d\n",
		total, time.Since(t0).Round(time.Millisecond), endRev, anchorRev+1)

	// Reconnect BELOW the ring's oldest event: the server must replay
	// [anchor+1, ...] from the event log (or scan fallback — the metrics diff
	// distinguishes them), then hand over to live tailing.
	t1 := time.Now()
	wch := cli.Watch(clientv3.WithRequireLeader(ctx), prefix,
		clientv3.WithPrefix(), clientv3.WithRev(anchorRev+1), clientv3.WithPrevKV())

	seen := map[string]int{} // value(seq marker) -> count
	var lastEvRev int64
	var firstBatchLatency time.Duration
	prevErrs, orderErrs := 0, 0
	deadline := time.After(120 * time.Second)
	got := 0
loop:
	for {
		select {
		case resp, ok := <-wch:
			if !ok {
				fmt.Println("FATAL: watch channel closed early")
				os.Exit(1)
			}
			if err := resp.Err(); err != nil {
				fmt.Printf("FATAL: watch error: %v\n", err)
				os.Exit(1)
			}
			if firstBatchLatency == 0 && len(resp.Events) > 0 {
				firstBatchLatency = time.Since(t1)
			}
			for _, ev := range resp.Events {
				key := string(ev.Kv.Key)
				if !strings.HasPrefix(key, prefix) {
					continue
				}
				if ev.Kv.ModRevision <= lastEvRev {
					orderErrs++
				}
				lastEvRev = ev.Kv.ModRevision
				seen[string(ev.Kv.Value)]++
				// PrevKv correctness: replay conversion resolves PrevKv through
				// the hint cache + slow path this campaign's review just changed.
				want, existed := expectPrev[key]
				if existed {
					if ev.PrevKv == nil || string(ev.PrevKv.Value) != want {
						prevErrs++
						if prevErrs <= 5 {
							fmt.Printf("  PREVKV-MISMATCH key=%s want=%q got=%v\n", key, want, ev.PrevKv)
						}
					}
				} else if ev.PrevKv != nil {
					prevErrs++
					if prevErrs <= 5 {
						fmt.Printf("  PREVKV-ON-CREATE key=%s got=%q\n", key, ev.PrevKv.Value)
					}
				}
				expectPrev[key] = string(ev.Kv.Value)
				got++
			}
			if got >= total {
				break loop
			}
		case <-deadline:
			fmt.Printf("FATAL: timeout with %d/%d events\n", got, total)
			os.Exit(1)
		}
	}
	catchup := time.Since(t1)

	// Completeness: every sequence marker exactly once.
	missing, dup := 0, 0
	for i := 0; i < total; i++ {
		switch seen[fmt.Sprintf("s%07d", i)] {
		case 1:
		case 0:
			missing++
		default:
			dup++
		}
	}
	fmt.Printf("\nRESULT total=%d received=%d missing=%d dup=%d order_errs=%d prevkv_errs=%d\n",
		total, got, missing, dup, orderErrs, prevErrs)
	fmt.Printf("first_batch=%s full_catchup=%s\n", firstBatchLatency.Round(time.Millisecond), catchup.Round(time.Millisecond))
	if missing == 0 && dup == 0 && orderErrs == 0 && prevErrs == 0 {
		fmt.Println("PASS")
		return
	}
	fmt.Println("FAIL")
	os.Exit(1)
}

func must(err error) {
	if err != nil {
		fmt.Println("FATAL:", err)
		os.Exit(1)
	}
}
