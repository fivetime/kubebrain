package compat

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Read-amplification BASELINE harness (P1). Drive KubeBrain with the official
// etcd client and read its Prometheus metrics to quantify backend storage
// round-trips per logical read. Point ENDPOINT and READ_AMP_METRICS_URL at the
// SAME (leader) pod for clean attribution, e.g. via:
//   kubectl -n kubebrain-dev port-forward pod/<leader> 13379:3379 18080:8080
//   READ_AMP=1 ENDPOINT=127.0.0.1:13379 READ_AMP_METRICS_URL=http://127.0.0.1:18080/metrics \
//     READ_AMP_N=2000 go test -run TestReadAmpBaseline -v -timeout 20m ./...
// It only measures + logs (no pass/fail); rerun after a fix to compare.

func metricsURL() string {
	if u := os.Getenv("READ_AMP_METRICS_URL"); u != "" {
		return u
	}
	return "http://127.0.0.1:18080/metrics"
}

// scrapeSum returns the summed value of all series whose metric name == name.
func scrapeSum(t *testing.T, name string) float64 {
	t.Helper()
	resp, err := http.Get(metricsURL())
	if err != nil {
		t.Fatalf("scrape %s: %v", metricsURL(), err)
	}
	defer resp.Body.Close()
	var sum float64
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1024*1024), 8*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		metric := line
		if i := strings.IndexAny(line, "{ "); i >= 0 {
			metric = line[:i]
		}
		if metric != name {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if v, err := strconv.ParseFloat(fields[len(fields)-1], 64); err == nil {
			sum += v
		}
	}
	return sum
}

const iterMetric = "storage_iter_start" // one per backend KvStorage.Iter call

func TestReadAmpBaseline(t *testing.T) {
	if os.Getenv("READ_AMP") == "" {
		t.Skip("set READ_AMP=1 to run the read-amplification baseline")
	}
	n := 2000
	if v := os.Getenv("READ_AMP_N"); v != "" {
		n, _ = strconv.Atoi(v)
	}
	const pageSize = 500
	value := strings.Repeat("x", 1024) // ~1KB, pod-ish

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{compatEndpoint()}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	ctx := context.Background()
	pfx := fmt.Sprintf("/registry/readamp/%d", time.Now().UnixNano())
	t.Cleanup(func() {
		c, cc := context.WithTimeout(context.Background(), 60*time.Second)
		defer cc()
		_, _ = cli.Delete(c, pfx, clientv3.WithPrefix())
	})

	// Populate N objects concurrently (sequential is too slow at large N).
	t.Logf("populating %d objects (~1KB) under %s ...", n, pfx)
	popStart := time.Now()
	{
		const workers = 24
		var wg sync.WaitGroup
		errCh := make(chan error, workers)
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				pc, perr := clientv3.New(clientv3.Config{Endpoints: []string{compatEndpoint()}, DialTimeout: 5 * time.Second})
				if perr != nil {
					errCh <- perr
					return
				}
				defer pc.Close()
				for i := w; i < n; i += workers {
					c, cc := context.WithTimeout(context.Background(), 15*time.Second)
					_, err := pc.Put(c, fmt.Sprintf("%s/obj-%06d", pfx, i), value)
					cc()
					if err != nil {
						errCh <- err
						return
					}
				}
			}(w)
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			t.Fatalf("populate: %v", err)
		}
	}
	t.Logf("populated in %v", time.Since(popStart))
	time.Sleep(1 * time.Second)

	measure := func(label string, fn func() (int, error)) {
		i0 := scrapeSum(t, iterMetric)
		start := time.Now()
		got, err := fn()
		dur := time.Since(start)
		i1 := scrapeSum(t, iterMetric)
		iters := i1 - i0
		ratio := 0.0
		if got > 0 {
			ratio = iters / float64(got)
		}
		t.Logf("[%s] returned=%d  wall=%v  backend_iter_ops=%.0f  iters/kv=%.2f", label, got, dur, iters, ratio)
		if err != nil {
			t.Logf("[%s] err=%v", label, err)
		}
	}

	// 1) Single LIST page (limit=pageSize) — exposes per-KV metadata round-trips (#3).
	measure(fmt.Sprintf("list-1page(limit=%d)", pageSize), func() (int, error) {
		r, err := cli.Get(ctx, pfx+"/", clientv3.WithPrefix(), clientv3.WithLimit(pageSize),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
		return len(r.Kvs), err
	})

	// 2) Full paginated LIST of all N (follow continue) — exposes O(N) metadata +
	//    O(N^2/page) exact-count scans (#5/#27).
	measure(fmt.Sprintf("list-full-paginated(n=%d,page=%d)", n, pageSize), func() (int, error) {
		total := 0
		key := pfx + "/"
		end := clientv3.GetPrefixRangeEnd(pfx + "/")
		for {
			r, err := cli.Get(ctx, key, clientv3.WithRange(end), clientv3.WithLimit(pageSize),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
			if err != nil {
				return total, err
			}
			total += len(r.Kvs)
			if !r.More || len(r.Kvs) == 0 {
				break
			}
			last := string(r.Kvs[len(r.Kvs)-1].Key)
			key = last + "\x00"
		}
		return total, nil
	})

	// 3) CountOnly — full scan streaming all values just to count (#29).
	measure("count-only", func() (int, error) {
		r, err := cli.Get(ctx, pfx+"/", clientv3.WithPrefix(), clientv3.WithCountOnly())
		return int(r.Count), err
	})

	// 4) Historical paginated LIST at a pinned revision — every page recomputes an
	//    exact count via a full unlimited scan (#5/#27).
	cur, _ := cli.Get(ctx, pfx+"/", clientv3.WithPrefix(), clientv3.WithLimit(1))
	rev := cur.Header.Revision
	measure(fmt.Sprintf("list-historical@rev=%d(page=%d)", rev, pageSize), func() (int, error) {
		total := 0
		key := pfx + "/"
		end := clientv3.GetPrefixRangeEnd(pfx + "/")
		for {
			r, err := cli.Get(ctx, key, clientv3.WithRange(end), clientv3.WithLimit(pageSize), clientv3.WithRev(rev),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
			if err != nil {
				return total, err
			}
			total += len(r.Kvs)
			if !r.More || len(r.Kvs) == 0 {
				break
			}
			key = string(r.Kvs[len(r.Kvs)-1].Key) + "\x00"
		}
		return total, nil
	})

	// 5) Watch fanout: W watchers, then M writes — metadata + prev-kv reads per
	//    event per watcher (#10/#28).
	const watchers, writes = 20, 100
	wctx, wcancel := context.WithCancel(ctx)
	for w := 0; w < watchers; w++ {
		ch := cli.Watch(wctx, pfx+"/", clientv3.WithPrefix(), clientv3.WithPrevKV())
		go func() {
			for range ch {
			}
		}()
	}
	time.Sleep(500 * time.Millisecond)
	i0 := scrapeSum(t, iterMetric)
	wstart := time.Now()
	for i := 0; i < writes; i++ {
		if _, err := cli.Put(ctx, fmt.Sprintf("%s/obj-%06d", pfx, i), value+"-upd"); err != nil {
			t.Fatalf("watch-write %d: %v", i, err)
		}
	}
	time.Sleep(3 * time.Second) // let fanout drain
	i1 := scrapeSum(t, iterMetric)
	wcancel()
	t.Logf("[watch-fanout] watchers=%d writes=%d wall=%v backend_iter_ops=%.0f iters/(write*watcher)=%.2f",
		watchers, writes, time.Since(wstart), i1-i0, (i1-i0)/float64(writes*watchers))
}
