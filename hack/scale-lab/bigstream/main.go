// bigstream: 直连 KubeBrain(etcd 协议)的大对象读写/流式压测工具。
// 用于 P1③ 大对象 RangeStream 缓冲实验(慢消费逼出服务端缓冲),也是
// 官方形态 benchmark(#61,300 client 直连 insert+delete)的雏形。
// put:    直连写 N 个 size 字节的 KV 到 prefix 下(绕开 apiserver)
// stream: GetStream(prefix) 并按 -sleep 每块慢消费,逼出服务端缓冲
// del:    DeleteRange 清理
package main

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	endpoint := flag.String("endpoint", "10.224.0.14:3379", "")
	mode := flag.String("mode", "put", "put|stream|del")
	n := flag.Int("n", 200000, "keys to put")
	size := flag.Int("size", 10240, "value bytes")
	workers := flag.Int("workers", 64, "")
	prefix := flag.String("prefix", "/registry/bigcm/", "")
	sleep := flag.Duration("sleep", 20*time.Millisecond, "per-chunk consumer sleep")
	flag.Parse()

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
