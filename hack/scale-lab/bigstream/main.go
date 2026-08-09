// bigstream: 直连 KubeBrain(etcd 协议)的大对象读写/流式压测工具。
// 用于 P1③ 大对象 RangeStream 缓冲实验(慢消费逼出服务端缓冲),也是
// 官方形态 benchmark(#61,300 client 直连 insert+delete)的雏形。
// put:    直连写 N 个 size 字节的 KV 到 prefix 下(绕开 apiserver)
// stream: GetStream(prefix) 并按 -sleep 每块慢消费,逼出服务端缓冲
// delprefix: GetStream 枚举 prefix 后并发逐 key Delete
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	endpoint := flag.String("endpoint", "10.224.0.14:3379", "")
	mode := flag.String("mode", "put", "put|stream|delprefix")
	n := flag.Int("n", 200000, "keys to put")
	size := flag.Int("size", 10240, "value bytes")
	workers := flag.Int("workers", 64, "")
	prefix := flag.String("prefix", "/registry/bigcm/", "")
	sleep := flag.Duration("sleep", 20*time.Millisecond, "per-chunk consumer sleep")
	flag.Parse()
	if err := validateMode(*mode); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{*endpoint}, DialTimeout: 10 * time.Second})
	if err != nil {
		panic(err)
	}
	defer cli.Close()
	ctx := context.Background()

	switch *mode {
	case "put":
		val := string(make([]byte, *size))
		var done, failed int64
		t0 := time.Now()
		var wg sync.WaitGroup
		ch := make(chan int, 1024)
		for w := 0; w < *workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range ch {
					if _, err := cli.Put(ctx, fmt.Sprintf("%skey-%07d", *prefix, i), val); err != nil {
						atomic.AddInt64(&failed, 1)
					} else {
						atomic.AddInt64(&done, 1)
					}
				}
			}()
		}
		go func() {
			for {
				time.Sleep(10 * time.Second)
				fmt.Printf("[put] done=%d failed=%d\n", atomic.LoadInt64(&done), atomic.LoadInt64(&failed))
			}
		}()
		for i := 0; i < *n; i++ {
			ch <- i
		}
		close(ch)
		wg.Wait()
		fmt.Printf("PUT DONE n=%d failed=%d elapsed=%s\n", done, failed, time.Since(t0).Round(time.Second))

	case "stream":
		t0 := time.Now()
		sc, err := cli.GetStream(ctx, *prefix, clientv3.WithPrefix())
		if err != nil {
			panic(err)
		}
		var chunks, keys, bytes int64
		for resp := range sc {
			if resp.Err() != nil {
				fmt.Printf("STREAM ERR after %d chunks: %v\n", chunks, resp.Err())
				return
			}
			chunks++
			for _, kv := range resp.Kvs {
				keys++
				bytes += int64(len(kv.Value))
			}
			time.Sleep(*sleep)
		}
		fmt.Printf("STREAM DONE chunks=%d keys=%d MB=%d elapsed=%s\n", chunks, keys, bytes/1048576, time.Since(t0).Round(time.Millisecond))

	case "delprefix":
		// 官方形态 delete 基准:GetStream 列出 prefix 下全部 key,
		// 再按 -workers 并发逐 key Delete,报 qps 与延迟分位。
		sc, err := cli.GetStream(ctx, *prefix, clientv3.WithPrefix())
		if err != nil {
			panic(err)
		}
		var keys []string
		for resp := range sc {
			if resp.Err() != nil {
				panic(resp.Err())
			}
			for _, kv := range resp.Kvs {
				keys = append(keys, string(kv.Key))
			}
		}
		fmt.Printf("[delprefix] keys=%d workers=%d\n", len(keys), *workers)
		lat := make([]time.Duration, len(keys))
		var done, failed int64
		t0 := time.Now()
		var wg sync.WaitGroup
		ch := make(chan int, 1024)
		for w := 0; w < *workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range ch {
					s := time.Now()
					if _, err := cli.Delete(ctx, keys[i]); err != nil {
						atomic.AddInt64(&failed, 1)
					} else {
						lat[i] = time.Since(s)
						atomic.AddInt64(&done, 1)
					}
				}
			}()
		}
		for i := range keys {
			ch <- i
		}
		close(ch)
		wg.Wait()
		elapsed := time.Since(t0)
		var oks []time.Duration
		for _, d := range lat {
			if d > 0 {
				oks = append(oks, d)
			}
		}
		sort.Slice(oks, func(i, j int) bool { return oks[i] < oks[j] })
		pct := func(p float64) time.Duration {
			if len(oks) == 0 {
				return 0
			}
			return oks[int(float64(len(oks)-1)*p)]
		}
		fmt.Printf("DELPREFIX DONE ok=%d failed=%d elapsed=%s qps=%.0f p50=%s p95=%s p99=%s\n",
			done, failed, elapsed.Round(time.Millisecond), float64(done)/elapsed.Seconds(),
			pct(0.50).Round(time.Microsecond), pct(0.95).Round(time.Microsecond), pct(0.99).Round(time.Microsecond))

	case "del":
		// 分键并发删,避免一笔 20 万 key 的巨事务
		var done int64
		var wg sync.WaitGroup
		ch := make(chan int, 1024)
		for w := 0; w < *workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range ch {
					if r, err := cli.Delete(ctx, fmt.Sprintf("%skey-%07d", *prefix, i)); err == nil {
						atomic.AddInt64(&done, r.Deleted)
					}
				}
			}()
		}
		for i := 0; i < *n; i++ {
			ch <- i
		}
		close(ch)
		wg.Wait()
		fmt.Printf("DEL DONE deleted=%d\n", done)
	}
}

func validateMode(mode string) error {
	switch mode {
	case "put", "stream", "delprefix":
		return nil
	default:
		return fmt.Errorf("unsupported mode %q (want put, stream, or delprefix)", mode)
	}
}
